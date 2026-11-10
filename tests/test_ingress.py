import asyncio
import json
from unittest.mock import AsyncMock

import httpx
import pytest

from mkk_bisness_test.api import create_app
from mkk_bisness_test.config import Settings
from mkk_bisness_test.ingress import MAX_BODY_BYTES, RequestGuard

API_KEY = "test-ingress-secret-key"


@pytest.fixture
def app():
    return create_app(
        Settings(
            database_url="postgresql+asyncpg://unused/unused",
            rabbitmq_url="amqp://unused/",
            api_key=API_KEY,
        )
    )


async def test_unauthenticated_body_is_never_read():
    downstream, receive, send = AsyncMock(), AsyncMock(), AsyncMock()
    scope = {
        "type": "http",
        "headers": [],
        "method": "POST",
        "path": "/api/v1/payments",
    }
    await RequestGuard(downstream, API_KEY)(scope, receive, send)
    receive.assert_not_awaited()
    downstream.assert_not_awaited()
    assert send.await_args_list[0].args[0]["status"] == 401


async def test_duplicate_credentials_are_rejected(app):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        response = await client.get(
            "/api/v1/payments/invalid",
            headers=[("X-API-Key", API_KEY), ("X-API-Key", API_KEY)],
        )
        assert response.status_code == 401


@pytest.mark.parametrize("chunked", [False, True])
async def test_request_limit_includes_streamed_bodies(app, chunked):
    async def chunks():
        for _ in range(9):
            yield b"x" * 8192

    content = chunks() if chunked else b"x" * (MAX_BODY_BYTES + 1)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/api/v1/payments",
            content=content,
            headers={"X-API-Key": API_KEY, "Content-Type": "application/json"},
        )
    assert response.status_code == 413
    assert len(response.content) < 200


async def test_slow_body_is_bounded(monkeypatch):
    monkeypatch.setattr("mkk_bisness_test.ingress.BODY_TIMEOUT_SECONDS", 0.01)
    downstream, send = AsyncMock(), AsyncMock()

    async def receive():
        await asyncio.sleep(0.1)
        return {"type": "http.request", "body": b"{}"}

    scope = {
        "type": "http",
        "headers": [(b"x-api-key", API_KEY.encode())],
        "method": "POST",
        "path": "/api/v1/payments",
    }
    await RequestGuard(downstream, API_KEY)(scope, receive, send)
    downstream.assert_not_awaited()
    assert send.await_args_list[0].args[0]["status"] == 408


@pytest.mark.parametrize(
    "field,value",
    [
        ("description", "bad\x00text"),
        ("description", "\ud800"),
        ("metadata", {"value": "bad\x00text"}),
        ("metadata", {"bad\x00key": "value"}),
        ("metadata", {"value": "\ud800"}),
        ("metadata", {"value": float("nan")}),
        ("metadata", {"value": float("inf")}),
        ("amount", float("nan")),
        ("amount", float("inf")),
        ("extra", "\ud800"),
    ],
)
async def test_invalid_values_return_bounded_422(
    app, request_data, field, value
):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/api/v1/payments",
            content=json.dumps({**request_data, field: value}),
            headers={
                "X-API-Key": API_KEY,
                "Idempotency-Key": "invalid",
                "Content-Type": "application/json",
            },
        )
    assert response.status_code == 422
    assert len(response.content) < 2000
    assert "input" not in response.json()["detail"][0]


async def test_validation_does_not_echo_input(app):
    marker = "sensitive-value-" * 1000
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/api/v1/payments",
            json={"extra": marker},
            headers={"X-API-Key": API_KEY, "Idempotency-Key": "invalid"},
        )
    assert response.status_code == 422
    assert "sensitive-value" not in response.text
    assert len(response.content) < 2000


async def test_metadata_depth_is_bounded(app, request_data):
    nested = "value"
    for _ in range(35):
        nested = {"nested": nested}
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/api/v1/payments",
            json={**request_data, "metadata": nested},
            headers={"X-API-Key": API_KEY, "Idempotency-Key": "invalid"},
        )
    assert response.status_code == 422
