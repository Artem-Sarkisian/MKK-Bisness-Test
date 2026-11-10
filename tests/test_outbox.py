from unittest.mock import AsyncMock

import pytest
from sqlalchemy import event, func, select

from mkk_bisness_test.models import Outbox, Payment
from mkk_bisness_test.outbox import OutboxRelay
from mkk_bisness_test.payments import PaymentService
from mkk_bisness_test.schemas import PaymentCreate

pytestmark = pytest.mark.integration


async def test_failed_publication_can_be_retried(database, payment):
    publisher = AsyncMock()
    publisher.publish.side_effect = [ConnectionError, None]
    relay = OutboxRelay(database, publisher, 0.1)
    with pytest.raises(ConnectionError):
        await relay.publish_one()
    async with database.sessions() as session:
        assert (await session.scalar(select(Outbox))).published_at is None
    assert await relay.publish_one()
    assert not await relay.publish_one()
    assert publisher.publish.await_count == 2
    async with database.sessions() as session:
        assert (await session.scalar(select(Outbox))).published_at is not None


async def test_payment_rollback_if_outbox_insert_fails(database, request_data):
    def fail_outbox(mapper, connection, target):
        raise RuntimeError("simulated insert failure")

    event.listen(Outbox, "before_insert", fail_outbox)
    try:
        with pytest.raises(RuntimeError):
            await PaymentService(database).create(
                PaymentCreate(**request_data), "rollback"
            )
    finally:
        event.remove(Outbox, "before_insert", fail_outbox)
    async with database.sessions() as session:
        assert (
            await session.scalar(select(func.count()).select_from(Payment))
            == 0
        )
        assert (
            await session.scalar(select(func.count()).select_from(Outbox)) == 0
        )
