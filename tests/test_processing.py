import asyncio
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from mkk_bisness_test.domain import DeliveryError, utcnow
from mkk_bisness_test.models import Outbox, Payment
from mkk_bisness_test.processing import PaymentProcessor
from mkk_bisness_test.schemas import PaymentJob

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("succeeded", [True, False])
async def test_result_and_concurrent_duplicate(database, payment, succeeded):
    gateway = AsyncMock()
    gateway.charge.return_value = succeeded
    sender = AsyncMock()
    processor = PaymentProcessor(database, gateway, sender, 2)
    job = PaymentJob(payment_id=payment.id, attempt=1)
    await asyncio.gather(processor.process(job), processor.process(job))
    gateway.charge.assert_awaited_once()
    sender.send.assert_awaited_once()
    async with database.sessions() as session:
        stored = await session.get(Payment, payment.id)
        assert stored.status == ("succeeded" if succeeded else "failed")
        assert stored.delivery_status == "delivered"
        assert stored.processed_at is not None
        assert stored.webhook_delivered_at is not None


async def test_retry_backoff_and_dlq_preserve_payment(database, payment):
    gateway = AsyncMock()
    gateway.charge.return_value = True
    sender = AsyncMock()
    sender.send.side_effect = DeliveryError("webhook_http_503")
    processor = PaymentProcessor(database, gateway, sender, 2)
    for attempt in (1, 2, 3):
        before = utcnow()
        await processor.process(
            PaymentJob(payment_id=payment.id, attempt=attempt)
        )
        async with database.sessions() as session:
            stored = await session.get(Payment, payment.id)
            assert stored.status == "succeeded"
            assert stored.attempts == attempt
            if attempt < 3:
                event = await session.scalar(
                    select(Outbox).where(
                        Outbox.dedupe_key == f"{payment.id}:{attempt + 1}"
                    )
                )
                assert (
                    event.available_at - before
                ).total_seconds() >= 2**attempt
            else:
                assert stored.delivery_status == "dead"
                dead = await session.scalar(
                    select(Outbox).where(Outbox.destination == "payments.dlq")
                )
                assert dead.payload["attempt"] == 3
    gateway.charge.assert_awaited_once()
    assert sender.send.await_count == 3
    assert (
        len({call.args[1]["event_id"] for call in sender.send.await_args_list})
        == 1
    )
    await processor.process(PaymentJob(payment_id=payment.id, attempt=3))
    assert sender.send.await_count == 3


async def test_recovery_and_stale_message(database, payment):
    gateway, sender = AsyncMock(), AsyncMock()
    sender.send.side_effect = [DeliveryError("webhook_network_error"), None]
    processor = PaymentProcessor(database, gateway, sender, 2)
    await processor.process(PaymentJob(payment_id=payment.id, attempt=1))
    # A new processor represents a restart; an old broker delivery is harmless.
    processor = PaymentProcessor(database, gateway, sender, 2)
    await processor.process(PaymentJob(payment_id=payment.id, attempt=1))
    await processor.process(PaymentJob(payment_id=payment.id, attempt=2))
    gateway.charge.assert_awaited_once()
    assert sender.send.await_count == 2
    async with database.sessions() as session:
        assert (
            await session.get(Payment, payment.id)
        ).delivery_status == "delivered"


async def test_interrupted_attempt_is_counted(database, payment):
    async with database.sessions.begin() as session:
        stored = await session.get(Payment, payment.id)
        stored.attempts = 1
        stored.status = "succeeded"
        stored.processed_at = utcnow()
    gateway, sender = AsyncMock(), AsyncMock()
    processor = PaymentProcessor(database, gateway, sender, 2)
    await processor.process(PaymentJob(payment_id=payment.id, attempt=1))
    gateway.charge.assert_not_awaited()
    sender.send.assert_not_awaited()
    async with database.sessions() as session:
        stored = await session.get(Payment, payment.id)
        assert stored.scheduled_attempt == 2
        assert stored.last_error == "interrupted_attempt"
    await processor.process(PaymentJob(payment_id=payment.id, attempt=2))
    gateway.charge.assert_not_awaited()
    sender.send.assert_awaited_once()
