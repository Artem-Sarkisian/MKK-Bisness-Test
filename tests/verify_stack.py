"""Verify a running Compose demo stack, including service interruption.

Run only against the local test stack: this script stops and starts its broker
and consumer. Requires compose.yaml + compose.test.yaml + compose.demo.yaml.
"""

import asyncio
import json
from uuid import UUID, uuid4

import httpx
from sqlalchemy import select

from mkk_bisness_test.config import get_settings
from mkk_bisness_test.database import Database
from mkk_bisness_test.messaging import (
    DEAD_QUEUE,
    MessagePublisher,
    create_broker,
)
from mkk_bisness_test.models import Outbox, Payment

COMPOSE = [
    "docker",
    "compose",
    "-f",
    "compose.yaml",
    "-f",
    "compose.test.yaml",
    "-f",
    "compose.demo.yaml",
]


async def compose(*arguments: str) -> None:
    process = await asyncio.create_subprocess_exec(
        *COMPOSE,
        *arguments,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    _, error = await process.communicate()
    if process.returncode:
        raise RuntimeError(error.decode())


async def eventually(check, deadline_seconds=60):
    async with asyncio.timeout(deadline_seconds):
        while True:
            result = await check()
            if result:
                return result
            await asyncio.sleep(0.2)


async def main() -> None:
    settings = get_settings()
    database = Database(settings.database_url)
    headers = {"X-API-Key": settings.api_key.get_secret_value()}
    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:8000", headers=headers, timeout=10
    ) as client:

        async def create(path="success", key=None):
            response = await client.post(
                "/api/v1/payments",
                headers={"Idempotency-Key": key or str(uuid4())},
                json={
                    "amount": "15.25",
                    "currency": "EUR",
                    "description": "Docker verification",
                    "metadata": {"source": "verification"},
                    "webhook_url": f"http://webhook:8080/{path}",
                },
            )
            assert response.status_code == 202, response.text
            return UUID(response.json()["payment_id"])

        async def stored(payment_id):
            async with database.sessions() as session:
                return await session.get(Payment, payment_id)

        async def terminal(payment_id, expected):
            async def check():
                payment = await stored(payment_id)
                return payment if payment.delivery_status == expected else None

            return await eventually(check)

        async def events(payment_id):
            async with httpx.AsyncClient(timeout=10) as receiver:
                response = await receiver.get("http://127.0.0.1:18081/events")
            return [
                e
                for e in response.json()
                if e["body"]["payment_id"] == str(payment_id)
            ]

        key = str(uuid4())
        ids = await asyncio.gather(*(create(key=key) for _ in range(8)))
        assert len(set(ids)) == 1
        success_id = ids[0]
        payment = await terminal(success_id, "delivered")
        assert payment.attempts == 1
        assert len(await events(success_id)) == 1
        result = await client.get(f"/api/v1/payments/{success_id}")
        assert result.status_code == 200 and result.json()["processed_at"]
        assert (
            await client.get(
                f"/api/v1/payments/{success_id}",
                headers={"X-API-Key": "wrong"},
            )
        ).status_code == 401
        print(
            "PASS HTTP API, concurrent idempotency, real webhook", flush=True
        )

        broker = create_broker(settings.rabbitmq_url)
        async with broker:
            publisher = MessagePublisher(broker)
            await publisher.setup()
            await publisher.publish(
                "payments.new",
                {"payment_id": str(success_id), "attempt": 1},
                str(uuid4()),
            )
        await asyncio.sleep(1)
        assert len(await events(success_id)) == 1
        print("PASS duplicate broker delivery", flush=True)

        flaky_id = await create("flaky")
        flaky = await terminal(flaky_id, "delivered")
        deliveries = await events(flaky_id)
        assert flaky.attempts == 3 and len(deliveries) == 3
        assert (
            len({json.dumps(e["body"], sort_keys=True) for e in deliveries})
            == 1
        )
        assert deliveries[1]["received_at"] - deliveries[0]["received_at"] >= 2
        assert deliveries[2]["received_at"] - deliveries[1]["received_at"] >= 4
        print(
            "PASS real 503 responses, 2/4-second backoff, recovery", flush=True
        )

        failed_id = await create("fail")
        failed = await terminal(failed_id, "dead")
        assert failed.attempts == 3 and len(await events(failed_id)) == 3
        broker = create_broker(settings.rabbitmq_url)
        async with broker:
            queue = await broker.declare_queue(DEAD_QUEUE)

            async def dead_received():
                message = await queue.get(fail=False)
                if not message:
                    return None
                body = json.loads(message.body)
                await message.ack()
                return (
                    body if body.get("payment_id") == str(failed_id) else None
                )

            dead = await eventually(dead_received)
            assert dead["attempt"] == 3
        print("PASS real DLQ after three failed webhook requests", flush=True)

        await compose("stop", "rabbitmq")
        try:
            queued_id = await create()
            await asyncio.sleep(2)
            async with database.sessions() as session:
                event = await session.scalar(
                    select(Outbox).where(Outbox.payment_id == queued_id)
                )
                assert event.published_at is None
            assert (await stored(queued_id)).status == "pending"
        finally:
            await compose("start", "rabbitmq")
        await terminal(queued_id, "delivered")
        print(
            "PASS API accepts during broker outage; Outbox recovers",
            flush=True,
        )

        outage_key = str(uuid4())
        await compose("stop", "postgres")
        try:
            response = await client.get(f"/api/v1/payments/{success_id}")
            assert response.status_code == 503
            assert response.json() == {
                "detail": "Storage temporarily unavailable"
            }
            response = await client.post(
                "/api/v1/payments",
                headers={"Idempotency-Key": outage_key},
                json={
                    "amount": "15.25",
                    "currency": "EUR",
                    "description": "Docker verification",
                    "metadata": {"source": "verification"},
                    "webhook_url": "http://webhook:8080/success",
                },
            )
            assert response.status_code == 503
        finally:
            await compose("start", "postgres")

        async def database_recovered():
            response = await client.get(f"/api/v1/payments/{success_id}")
            return response.status_code == 200

        await eventually(database_recovered)
        recovered_id = await create(key=outage_key)
        assert await create(key=outage_key) == recovered_id
        await terminal(recovered_id, "delivered")
        print(
            "PASS PostgreSQL outage returns503; API and worker recover",
            flush=True,
        )

        interrupted_id = await create("slow")
        await eventually(lambda: events(interrupted_id))
        before = await stored(interrupted_id)
        try:
            await compose("kill", "-s", "SIGKILL", "consumer")
        finally:
            await compose("start", "consumer")
        interrupted = await terminal(interrupted_id, "dead")
        assert interrupted.processed_at == before.processed_at
        assert interrupted.status == before.status
        assert interrupted.attempts == 3
        assert len(await events(interrupted_id)) == 3
        print(
            "PASS consumer crash; result and attempt budget survive",
            flush=True,
        )
    await database.close()
    print("All running-stack checks passed.", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
