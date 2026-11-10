import asyncio
import secrets

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

MAX_BODY_BYTES = 65536
BODY_TIMEOUT_SECONDS = 10


class RequestGuard:
    """Authenticate and bound the body before framework JSON decoding."""

    def __init__(self, app: ASGIApp, api_key: str) -> None:
        self.app = app
        self.api_key = api_key.encode()

    async def __call__(
        self, scope: Scope, receive: Receive, send: Send
    ) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        keys = [v for k, v in scope["headers"] if k == b"x-api-key"]
        if len(keys) != 1 or not secrets.compare_digest(keys[0], self.api_key):
            await self._reject(401, "Invalid API key", scope, receive, send)
            return
        headers = dict(scope["headers"])
        if headers.get(b"content-encoding", b"identity") != b"identity":
            await self._reject(
                415, "Content encoding is not supported", scope, receive, send
            )
            return
        try:
            length = int(headers.get(b"content-length", b"0"))
        except ValueError:
            await self._reject(
                400, "Invalid content length", scope, receive, send
            )
            return
        if length < 0 or length > MAX_BODY_BYTES:
            await self._reject(
                413, "Request body too large", scope, receive, send
            )
            return

        body = bytearray()
        try:
            async with asyncio.timeout(BODY_TIMEOUT_SECONDS):
                while True:
                    message = await receive()
                    if message["type"] == "http.disconnect":
                        return
                    chunk = message.get("body", b"")
                    if len(body) + len(chunk) > MAX_BODY_BYTES:
                        await self._reject(
                            413, "Request body too large", scope, receive, send
                        )
                        return
                    body.extend(chunk)
                    if not message.get("more_body", False):
                        break
        except TimeoutError:
            await self._reject(
                408, "Request body timed out", scope, receive, send
            )
            return

        consumed = False

        async def bounded_receive() -> Message:
            nonlocal consumed
            if not consumed:
                consumed = True
                return {"type": "http.request", "body": bytes(body)}
            return await receive()

        await self.app(scope, bounded_receive, send)

    @staticmethod
    async def _reject(
        status: int, detail: str, scope: Scope, receive: Receive, send: Send
    ) -> None:
        response = JSONResponse(
            {"detail": detail},
            status_code=status,
            headers={"Connection": "close"},
        )
        await response(scope, receive, send)
