from faststream.rabbit import (
    Channel,
    RabbitBroker,
    RabbitExchange,
    RabbitQueue,
)
from pamqp.commands import Basic

EXCHANGE = RabbitExchange("payments", durable=True)
DEAD_EXCHANGE = RabbitExchange("payments.dead", durable=True)
QUEUE = RabbitQueue("payments.new", durable=True, routing_key="payments.new")
DEAD_QUEUE = RabbitQueue(
    "payments.dlq", durable=True, routing_key="payments.dlq"
)


class MessagePublisher:
    def __init__(self, broker: RabbitBroker) -> None:
        self.broker = broker

    async def setup(self) -> None:
        for queue, exchange in (
            (QUEUE, EXCHANGE),
            (DEAD_QUEUE, DEAD_EXCHANGE),
        ):
            declared_exchange = await self.broker.declare_exchange(exchange)
            declared_queue = await self.broker.declare_queue(queue)
            await declared_queue.bind(
                declared_exchange, routing_key=queue.routing_key
            )

    async def publish(
        self, destination: str, payload: dict, event_id: str
    ) -> None:
        if destination not in {QUEUE.name, DEAD_QUEUE.name}:
            raise ValueError("unknown_destination")
        confirmation = await self.broker.publish(
            payload,
            exchange=DEAD_EXCHANGE
            if destination == DEAD_QUEUE.name
            else EXCHANGE,
            routing_key=destination,
            persist=True,
            mandatory=True,
            timeout=5,
            message_id=event_id,
        )
        if not isinstance(confirmation, Basic.Ack):
            raise RuntimeError("publication_not_confirmed")


def create_broker(url: str) -> RabbitBroker:
    return RabbitBroker(
        url,
        default_channel=Channel(
            prefetch_count=1, publisher_confirms=True, on_return_raises=True
        ),
    )
