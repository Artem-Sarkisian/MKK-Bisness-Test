import asyncio
import json
import os
from uuid import uuid4

import pytest
from aio_pika import Message, connect_robust
from aio_pika.exceptions import DeliveryError
from sqlalchemy import select

from mkk_bisness_test.config import get_settings
from mkk_bisness_test.consumer import build_app
from mkk_bisness_test.messaging import (
    DEAD_QUEUE,
    MessagePublisher,
    create_broker,
)
from mkk_bisness_test.models import Outbox, Payment
from mkk_bisness_test.outbox import OutboxRelay

pytestmark = pytest.mark.integration


async def wait_for(check, seconds=15):
    async with asyncio.timeout(seconds):
        while True:
            result = await check()
            if result:
                return result
            await asyncio.sleep(0.05)


@pytest.fixture
def broker_url():
    url = os.getenv("TEST_RABBITMQ_URL")
    if not url:
        pytest.skip("TEST_RABBITMQ_URL is required")
    if not url.endswith("/payments_test"):
        raise RuntimeError(
            "Broker tests require the payments_test virtual host"
        )
    return url


async def test_confirmed_routing_and_failed_route(
    database, payment, broker_url
):
    broker = create_broker(broker_url)
    async with broker:
        publisher = MessagePublisher(broker)
        await publisher.setup()
        queue = await broker.declare_queue(DEAD_QUEUE)
        await queue.purge()
        await publisher.publish("payments.dlq", {"test": True}, str(uuid4()))
        received = await queue.get(timeout=2)
        assert json.loads(received.body) == {"test": True}
        await received.ack()
        # Missing queue bindings must leave Outbox unpublished.
        connection = await connect_robust(broker_url)
        try:
            channel = await connection.channel()
            main = await channel.get_queue("payments.new")
            await main.unbind("payments", routing_key="payments.new")
            relay = OutboxRelay(database, publisher, 0.1)
            with pytest.raises(DeliveryError):
                await relay.publish_one()
            async with database.sessions() as session:
                assert (
                    await session.scalar(select(Outbox))
                ).published_at is None
            await publisher.setup()
            assert await relay.publish_one()
            await main.purge()
        finally:
            await connection.close()


async def test_real_consumer_retries_and_dead_letters(
    database, payment, broker_url, monkeypatch
):
    monkeypatch.setenv(
        "DATABASE_URL",
        database.engine.url.render_as_string(hide_password=False),
    )
    monkeypatch.setenv("RABBITMQ_URL", broker_url)
    monkeypatch.setenv("API_KEY", "integration-test-api-key")
    monkeypatch.setenv("RETRY_BASE_SECONDS", "0.05")
    monkeypatch.setenv("OUTBOX_POLL_SECONDS", "0.02")
    monkeypatch.setenv("WEBHOOK_ALLOWED_HOSTS", "[]")
    async with database.sessions.begin() as session:
        stored = await session.get(Payment, payment.id)
        stored.webhook_url = "http://127.0.0.1:1/blocked"
    get_settings.cache_clear()
    app = build_app()
    connection = await connect_robust(broker_url)
    try:
        await app.start()
        channel = await connection.channel()
        dead = await channel.get_queue("payments.dlq")
        await dead.purge()

        async def read_dead():
            return await dead.get(fail=False)

        message = await wait_for(read_dead)
        body = json.loads(message.body)
        assert body["payment_id"] == str(payment.id)
        assert body["attempt"] == 3
        await message.ack()
        async with database.sessions() as session:
            stored = await session.get(Payment, payment.id)
            assert stored.status in {"succeeded", "failed"}
            assert stored.attempts == 3
            assert stored.delivery_status == "dead"
        # Invalid JSON must reach the handler, not fail during decoding.
        exchange = await channel.get_exchange("payments")
        await exchange.publish(
            Message(b"{broken", content_type="application/json"),
            routing_key="payments.new",
        )
        invalid = await wait_for(read_dead)
        assert json.loads(invalid.body)["reason"] == "invalid_message"
        await invalid.ack()
    finally:
        await app.stop()
        await connection.close()
        get_settings.cache_clear()
