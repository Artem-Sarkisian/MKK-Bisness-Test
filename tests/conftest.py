import os

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import text

from mkk_bisness_test.api import create_app
from mkk_bisness_test.config import Settings
from mkk_bisness_test.database import Database
from mkk_bisness_test.payments import PaymentService
from mkk_bisness_test.schemas import PaymentCreate

API_KEY = "integration-test-api-key"


@pytest.fixture
def request_data():
    return {
        "amount": "123.45",
        "currency": "RUB",
        "description": "Order 42",
        "metadata": {"order_id": 42},
        "webhook_url": "https://example.com/payments",
    }


@pytest_asyncio.fixture
async def database():
    url = os.getenv("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL is required")
    if not url.rsplit("/", 1)[-1].endswith("_test"):
        raise RuntimeError("Tests require a separate database ending in _test")
    db = Database(url)
    async with db.engine.begin() as connection:
        await connection.execute(text("TRUNCATE outbox, payments CASCADE"))
    yield db
    await db.close()


@pytest_asyncio.fixture
async def payment(database, request_data):
    return await PaymentService(database).create(
        PaymentCreate(**request_data), "test-key"
    )


@pytest_asyncio.fixture
async def client(database):
    app = create_app(
        Settings(
            database_url=str(
                database.engine.url.render_as_string(hide_password=False)
            ),
            rabbitmq_url="amqp://unused/",
            api_key=API_KEY,
        )
    )
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
            headers={"X-API-Key": API_KEY},
        ) as client:
            yield client
