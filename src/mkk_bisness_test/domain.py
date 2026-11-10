from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Protocol
from uuid import UUID


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class DeliveryStatus(StrEnum):
    PENDING = "pending"
    DELIVERED = "delivered"
    DEAD = "dead"


class IdempotencyConflict(Exception):
    pass


class DeliveryError(Exception):
    """A sanitized error safe to include in operational logs."""


class Gateway(Protocol):
    async def charge(
        self, payment_id: UUID, amount: Decimal, currency: str
    ) -> bool: ...


class WebhookSender(Protocol):
    async def send(self, url: str, payload: dict) -> None: ...


def utcnow() -> datetime:
    return datetime.now(UTC)
