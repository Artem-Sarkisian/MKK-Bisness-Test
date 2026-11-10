from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


class Database:
    def __init__(self, url: str) -> None:
        self.engine = create_async_engine(
            url,
            pool_pre_ping=True,
            hide_parameters=True,
            pool_timeout=5,
            connect_args={"timeout": 5, "command_timeout": 15},
        )
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)

    @asynccontextmanager
    async def payment_lock(self, payment_id: UUID) -> AsyncIterator[None]:
        # This transaction-scoped lock is released on connection loss.
        # Separate short transactions persist progress while this lock is held.
        key = int.from_bytes(payment_id.bytes[:8], signed=True)
        async with self.engine.begin() as connection:
            await connection.execute(
                text("SELECT pg_advisory_xact_lock(:key)"), {"key": key}
            )
            yield

    async def close(self) -> None:
        await self.engine.dispose()
