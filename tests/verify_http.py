"""Exercise input boundaries through the running HTTP server and PostgreSQL."""

import asyncio
import json
from uuid import UUID, uuid4

import httpx
from sqlalchemy import func, select

from mkk_bisness_test.config import get_settings
from mkk_bisness_test.database import Database
from mkk_bisness_test.models import Payment


async def main() -> None:
    settings = get_settings()
    database = Database(settings.database_url)
    key = settings.api_key.get_secret_value()
    base = {
        "amount": "1.00",
        "currency": "RUB",
        "description": "HTTP verification",
        "metadata": {},
        "webhook_url": "http://webhook:8080/success",
    }
    headers = {
        "X-API-Key": key,
        "Content-Type": "application/json",
        "Idempotency-Key": str(uuid4()),
    }
    async with database.sessions() as session:
        before = await session.scalar(
            select(func.count()).select_from(Payment)
        )
    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:8000", timeout=15
    ) as client:
        for supplied in ({}, {"X-API-Key": "wrong"}):
            response = await client.post(
                "/api/v1/payments",
                content=b"{broken",
                headers={"Content-Type": "application/json", **supplied},
            )
            assert response.status_code == 401
        response = await client.get(
            "/api/v1/payments/invalid",
            headers=[("X-API-Key", key), ("X-API-Key", key)],
        )
        assert response.status_code == 401
        print(
            "PASS early authentication over HTTP, including duplicate headers",
            flush=True,
        )

        for field, value in [
            ("description", "a\x00b"),
            ("description", "\ud800"),
            ("metadata", {"bad": "a\x00b"}),
            ("metadata", {"\ud800": "value"}),
            ("metadata", {"bad": float("nan")}),
            ("metadata", {"bad": float("inf")}),
            ("amount", float("nan")),
            ("amount", "0"),
            ("amount", "1.001"),
            ("currency", "BTC"),
            ("webhook_url", "file:///etc/passwd"),
            ("webhook_url", "https://user:password@example.com"),
        ]:
            response = await client.post(
                "/api/v1/payments",
                content=json.dumps({**base, field: value}),
                headers=headers,
            )
            assert response.status_code == 422, (field, response.status_code)
            assert len(response.content) < 2000
            assert all(
                "input" not in item for item in response.json()["detail"]
            )
        print(
            "PASS invalid Unicode, JSON numbers, amounts and URLs return422",
            flush=True,
        )

        response = await client.post(
            "/api/v1/payments", content=b"x" * 65537, headers=headers
        )
        assert response.status_code == 413

        async def chunks():
            for _ in range(9):
                yield b"x" * 8192

        response = await client.post(
            "/api/v1/payments", content=chunks(), headers=headers
        )
        assert response.status_code == 413
        response = await client.post(
            "/api/v1/payments",
            content=b"{}",
            headers={**headers, "Content-Encoding": "gzip"},
        )
        assert response.status_code == 415
        print(
            "PASS body limits and unsupported content encoding",
            flush=True,
        )

    reader, writer = await asyncio.open_connection("127.0.0.1", 8000)
    try:
        writer.write(
            (
                "POST /api/v1/payments HTTP/1.1\r\nHost: localhost\r\n"
                + f"X-API-Key: {key}\r\n"
                + "Content-Type: application/json\r\n"
                + "Content-Length: 10\r\n\r\n{"
            ).encode()
        )
        await writer.drain()
        async with asyncio.timeout(12):
            response = await reader.read()
        assert response.startswith(b"HTTP/1.1 408"), response[:100]
    finally:
        writer.close()
        await writer.wait_closed()
    print("PASS slow incomplete HTTP body times out", flush=True)
    async with database.sessions() as session:
        after = await session.scalar(select(func.count()).select_from(Payment))
    assert after == before
    print("PASS rejected requests created no payment rows", flush=True)
    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:8000", timeout=15
    ) as client:
        for destination in ("http://127.0.0.1:8080/", "http://postgres:5432/"):
            response = await client.post(
                "/api/v1/payments",
                json={**base, "webhook_url": destination},
                headers={**headers, "Idempotency-Key": str(uuid4())},
            )
            assert response.status_code == 202
            payment_id = UUID(response.json()["payment_id"])
            async with asyncio.timeout(25):
                while True:
                    async with database.sessions() as session:
                        payment = await session.get(Payment, payment_id)
                    if payment.delivery_status == "dead":
                        break
                    await asyncio.sleep(0.2)
            assert payment.last_error == "webhook_address_blocked"
            assert payment.attempts == 3
    await database.close()
    print(
        "PASS real consumer blocks loopback and private DNS targets",
        flush=True,
    )


if __name__ == "__main__":
    asyncio.run(main())
