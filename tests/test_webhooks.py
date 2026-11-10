import asyncio
from unittest.mock import AsyncMock

import pytest
from aiohttp import web

from mkk_bisness_test.domain import DeliveryError
from mkk_bisness_test.webhooks import HttpWebhookSender, PublicAddressResolver


@pytest.mark.parametrize(
    "address",
    [
        "127.0.0.1",
        "10.0.0.1",
        "169.254.169.254",
        "::1",
        "::ffff:127.0.0.1",
        "0.0.0.0",
        "ff02::1",
        "224.0.0.1",
    ],
)
async def test_private_addresses_blocked(address):
    resolver = PublicAddressResolver([])
    try:
        with pytest.raises(DeliveryError):
            resolver.validate("example.com", address)
    finally:
        await resolver.close()


async def test_dns_result_validated():
    resolver = PublicAddressResolver([])
    resolver.resolver.resolve = AsyncMock(return_value=[{"host": "127.0.0.1"}])
    try:
        with pytest.raises(DeliveryError):
            await resolver.resolve("example.com")
    finally:
        await resolver.close()


@pytest.mark.parametrize("status", [200, 204, 302, 400, 429, 500])
async def test_http_delivery_and_no_redirects(status):
    received = []

    async def receive(request):
        received.append(await request.json())
        return web.Response(
            status=status, headers={"Location": "http://127.0.0.1:1/"}
        )

    app = web.Application()
    app.router.add_post("/", receive)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = runner.addresses[0][1]
    try:
        sender = HttpWebhookSender(1, ["127.0.0.1"])
        if status < 300:
            await sender.send(
                f"http://127.0.0.1:{port}/", {"event_id": "same"}
            )
        else:
            with pytest.raises(DeliveryError, match=f"webhook_http_{status}"):
                await sender.send(
                    f"http://127.0.0.1:{port}/", {"event_id": "same"}
                )
        assert received == [{"event_id": "same"}]
    finally:
        await runner.cleanup()


async def test_timeout():
    async def receive(request):
        await asyncio.sleep(0.2)
        return web.Response(status=200)

    app = web.Application()
    app.router.add_post("/", receive)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    try:
        with pytest.raises(DeliveryError, match="webhook_network_error"):
            await HttpWebhookSender(0.01, ["127.0.0.1"]).send(
                f"http://127.0.0.1:{runner.addresses[0][1]}/", {}
            )
    finally:
        await runner.cleanup()
