import asyncio
import random
from decimal import Decimal
from uuid import UUID


class SimulatedGateway:
    async def charge(
        self, payment_id: UUID, amount: Decimal, currency: str
    ) -> bool:
        await asyncio.sleep(random.uniform(2, 5))
        return random.random() < 0.9
