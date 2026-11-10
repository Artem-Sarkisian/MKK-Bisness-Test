from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from mkk_bisness_test.gateway import SimulatedGateway


@pytest.mark.parametrize(
    "random_value,expected",
    [(0.0, True), (0.8999, True), (0.9, False), (0.999, False)],
)
async def test_gateway_probability_boundary_and_delay(
    monkeypatch, random_value, expected
):
    sleep = AsyncMock()
    monkeypatch.setattr("mkk_bisness_test.gateway.asyncio.sleep", sleep)
    monkeypatch.setattr(
        "mkk_bisness_test.gateway.random.random", lambda: random_value
    )
    assert (
        await SimulatedGateway().charge(uuid4(), Decimal("1.00"), "RUB")
        is expected
    )
    assert 2 <= sleep.await_args.args[0] <= 5
