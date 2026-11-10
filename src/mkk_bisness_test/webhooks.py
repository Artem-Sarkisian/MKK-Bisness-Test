import ipaddress
import socket
from urllib.parse import urlsplit

import aiohttp
from aiohttp.abc import AbstractResolver
from aiohttp.resolver import ThreadedResolver

from mkk_bisness_test.domain import DeliveryError


class PublicAddressResolver(AbstractResolver):
    def __init__(self, allowed_hosts: list[str]) -> None:
        self.resolver = ThreadedResolver()
        self.allowed_hosts = {
            host.lower().rstrip(".") for host in allowed_hosts
        }

    def validate(self, host: str, address: str) -> None:
        if host.lower().rstrip(".") in self.allowed_hosts:
            return
        ip = ipaddress.ip_address(address)
        if (
            not ip.is_global
            or ip.is_multicast
            or (
                ip.version == 6
                and ip.ipv4_mapped
                and not ip.ipv4_mapped.is_global
            )
        ):
            raise DeliveryError("webhook_address_blocked")

    async def resolve(
        self, host: str, port: int = 0, family: int = socket.AF_INET
    ) -> list:
        addresses = await self.resolver.resolve(host, port, family)
        for address in addresses:
            self.validate(host, address["host"])
        return addresses

    async def close(self) -> None:
        await self.resolver.close()


class HttpWebhookSender:
    def __init__(self, timeout: float, allowed_hosts: list[str]) -> None:
        self.timeout = timeout
        self.allowed_hosts = allowed_hosts

    async def send(self, url: str, payload: dict) -> None:
        parsed = urlsplit(url)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username
            or parsed.password
        ):
            raise DeliveryError("webhook_url_invalid")
        resolver = PublicAddressResolver(self.allowed_hosts)
        try:
            try:
                literal = ipaddress.ip_address(parsed.hostname)
            except ValueError:
                pass
            else:
                resolver.validate(parsed.hostname, str(literal))
            # Use validated DNS results without a second lookup.
            connector = aiohttp.TCPConnector(
                resolver=resolver, use_dns_cache=False
            )
            async with aiohttp.ClientSession(
                connector=connector,
                timeout=aiohttp.ClientTimeout(total=self.timeout),
                trust_env=False,
            ) as client:
                async with client.post(
                    url, json=payload, allow_redirects=False
                ) as response:
                    if not 200 <= response.status < 300:
                        raise DeliveryError(f"webhook_http_{response.status}")
        except TimeoutError, aiohttp.ClientError, OSError:
            raise DeliveryError("webhook_network_error") from None
        finally:
            await resolver.close()
