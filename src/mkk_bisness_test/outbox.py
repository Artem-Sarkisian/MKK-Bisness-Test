import asyncio
import logging

from sqlalchemy import select

from mkk_bisness_test.database import Database
from mkk_bisness_test.domain import utcnow
from mkk_bisness_test.messaging import MessagePublisher
from mkk_bisness_test.models import Outbox

logger = logging.getLogger(__name__)


class OutboxRelay:
    def __init__(
        self,
        database: Database,
        publisher: MessagePublisher,
        poll_seconds: float,
    ) -> None:
        self.database = database
        self.publisher = publisher
        self.poll_seconds = poll_seconds

    async def publish_one(self) -> bool:
        async with self.database.sessions.begin() as session:
            event = (
                await session.execute(
                    select(Outbox)
                    .where(
                        Outbox.published_at.is_(None),
                        Outbox.available_at <= utcnow(),
                    )
                    .order_by(Outbox.available_at)
                    .with_for_update(skip_locked=True)
                    .limit(1)
                )
            ).scalar_one_or_none()
            if event is None:
                return False
            await self.publisher.publish(
                event.destination, event.payload, str(event.id)
            )
            event.published_at = utcnow()
            return True

    async def run(self) -> None:
        while True:
            try:
                if await self.publish_one():
                    continue
            except Exception as error:
                logger.warning(
                    "outbox_publish_failed error_type=%s", type(error).__name__
                )
            await asyncio.sleep(self.poll_seconds)
