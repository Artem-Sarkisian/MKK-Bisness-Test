import asyncio
import hashlib
import logging

from faststream import AckPolicy, FastStream
from faststream.rabbit.annotations import RabbitMessage
from pydantic import ValidationError

from mkk_bisness_test.config import get_settings
from mkk_bisness_test.database import Database
from mkk_bisness_test.gateway import SimulatedGateway
from mkk_bisness_test.messaging import (
    EXCHANGE,
    QUEUE,
    MessagePublisher,
    create_broker,
)
from mkk_bisness_test.outbox import OutboxRelay
from mkk_bisness_test.processing import PaymentProcessor
from mkk_bisness_test.schemas import PaymentJob
from mkk_bisness_test.webhooks import HttpWebhookSender

logger = logging.getLogger(__name__)


def build_app() -> FastStream:
    settings = get_settings()
    database = Database(settings.database_url)
    broker = create_broker(settings.rabbitmq_url)
    publisher = MessagePublisher(broker)
    processor = PaymentProcessor(
        database,
        SimulatedGateway(),
        HttpWebhookSender(
            settings.webhook_timeout, settings.webhook_allowed_hosts
        ),
        settings.retry_base_seconds,
    )
    relay = OutboxRelay(database, publisher, settings.outbox_poll_seconds)
    app = FastStream(broker)
    relay_task: asyncio.Task | None = None

    async def decode_body(message) -> bytes:
        return message.body

    @broker.subscriber(
        QUEUE,
        EXCHANGE,
        ack_policy=AckPolicy.MANUAL,
        no_reply=True,
        decoder=decode_body,
    )
    async def handle_payment(message: RabbitMessage) -> None:
        try:
            try:
                job = PaymentJob.model_validate_json(message.body)
                await processor.process(job)
            except ValidationError, ValueError:
                # Keep untrusted bodies and secrets out of diagnostic events.
                digest = hashlib.sha256(message.body).hexdigest()
                await publisher.publish(
                    "payments.dlq",
                    {"reason": "invalid_message", "body_sha256": digest},
                    digest,
                )
            await message.ack()
        except Exception as error:
            logger.warning(
                "message_deferred error_type=%s", type(error).__name__
            )
            await asyncio.sleep(1)
            await message.nack(requeue=True)

    @app.after_startup
    async def startup() -> None:
        nonlocal relay_task
        await publisher.setup()
        relay_task = asyncio.create_task(relay.run())

    @app.on_shutdown
    async def shutdown() -> None:
        if relay_task is not None:
            relay_task.cancel()
            await asyncio.gather(relay_task, return_exceptions=True)
        await database.close()

    return app


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(build_app().run())


if __name__ == "__main__":
    main()
