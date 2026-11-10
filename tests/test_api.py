import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from mkk_bisness_test.models import Outbox, Payment

pytestmark = pytest.mark.integration


async def test_create_get_and_idempotent_replay(
    client, database, request_data
):
    headers = {"Idempotency-Key": "order-42"}
    results = await asyncio.gather(
        *(
            client.post("/api/v1/payments", json=request_data, headers=headers)
            for _ in range(8)
        )
    )
    assert {r.status_code for r in results} == {202}
    ids = {r.json()["payment_id"] for r in results}
    assert len(ids) == 1
    result = await client.get(f"/api/v1/payments/{ids.pop()}")
    assert result.status_code == 200
    assert result.json()["amount"] == "123.45"
    assert result.json()["metadata"] == {"order_id": 42}
    assert result.json()["processed_at"] is None
    async with database.sessions() as session:
        assert (
            await session.scalar(select(func.count()).select_from(Payment))
            == 1
        )
        assert (
            await session.scalar(select(func.count()).select_from(Outbox)) == 1
        )
    changed = await client.post(
        "/api/v1/payments",
        json={**request_data, "amount": "200"},
        headers=headers,
    )
    assert changed.status_code == 409


async def test_normalized_amount_and_metadata_order(client, request_data):
    request_data.update(amount="10.0", metadata={"a": 1, "b": 2})
    first = await client.post(
        "/api/v1/payments",
        json=request_data,
        headers={"Idempotency-Key": "normalized"},
    )
    request_data.update(amount="10.00", metadata={"b": 2, "a": 1})
    second = await client.post(
        "/api/v1/payments",
        json=request_data,
        headers={"Idempotency-Key": "normalized"},
    )
    assert first.json() == second.json()


@pytest.mark.parametrize(
    "field,value",
    [
        ("amount", "0"),
        ("amount", "-1"),
        ("amount", "1.001"),
        ("amount", "NaN"),
        ("amount", "10000000000000000"),
        ("currency", "BTC"),
        ("webhook_url", "file:///etc/passwd"),
        ("webhook_url", "https://user:pass@example.com"),
        ("description", "x" * 1001),
        ("metadata", {"x": "a" * 17000}),
    ],
)
async def test_invalid_data(client, request_data, field, value):
    result = await client.post(
        "/api/v1/payments",
        json={**request_data, field: value},
        headers={"Idempotency-Key": "invalid"},
    )
    assert result.status_code == 422


async def test_auth_missing_key_and_not_found(client, request_data):
    for path in ("/api/v1/payments", f"/api/v1/payments/{uuid4()}"):
        method = client.post if path.endswith("payments") else client.get
        response = await method(path, headers={"X-API-Key": "incorrect"})
        assert response.status_code == 401
    client.headers.clear()
    assert (await client.get(f"/api/v1/payments/{uuid4()}")).status_code == 401
    client.headers["X-API-Key"] = "integration-test-api-key"
    assert (
        await client.post("/api/v1/payments", json=request_data)
    ).status_code == 422
    assert (await client.get(f"/api/v1/payments/{uuid4()}")).status_code == 404
    for path in ("/docs", "/redoc", "/openapi.json"):
        assert (await client.get(path)).status_code == 404


@pytest.mark.parametrize("currency", ["RUB", "USD", "EUR"])
async def test_amount_boundary_and_arbitrary_text(
    client, request_data, currency
):
    description = "'; DROP TABLE payments; -- <script>alert(1)</script>"
    request_data.update(
        amount="9999999999999999.99",
        currency=currency,
        description=description,
    )
    first = await client.post(
        "/api/v1/payments",
        json=request_data,
        headers={"Idempotency-Key": "boundary"},
    )
    assert first.status_code == 202
    detail = await client.get(f"/api/v1/payments/{first.json()['payment_id']}")
    assert detail.status_code == 200
    assert detail.json()["description"] == description
    assert detail.json()["currency"] == currency
    assert detail.json()["amount"] == "9999999999999999.99"
