from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Numeric,
    String,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from mkk_bisness_test.domain import DeliveryStatus, PaymentStatus, utcnow


class Base(DeclarativeBase):
    pass


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (
        CheckConstraint("amount > 0", name="payment_amount_positive"),
        CheckConstraint(
            "currency IN ('RUB', 'USD', 'EUR')", name="payment_currency"
        ),
        CheckConstraint(
            "status IN ('pending', 'succeeded', 'failed')",
            name="payment_status",
        ),
        CheckConstraint(
            "delivery_status IN ('pending', 'delivered', 'dead')",
            name="delivery_status",
        ),
        CheckConstraint("attempts BETWEEN 0 AND 3", name="payment_attempts"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[str] = mapped_column(String(3))
    description: Mapped[str] = mapped_column(String(1000))
    metadata_: Mapped[dict] = mapped_column("metadata", JSONB)
    status: Mapped[str] = mapped_column(
        String(16), default=PaymentStatus.PENDING
    )
    idempotency_key: Mapped[str] = mapped_column(String(128), unique=True)
    request_hash: Mapped[str] = mapped_column(String(64))
    webhook_url: Mapped[str] = mapped_column(String(2048))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )
    processed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    webhook_event_id: Mapped[UUID] = mapped_column(default=uuid4, unique=True)
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    delivery_status: Mapped[str] = mapped_column(
        String(16), default=DeliveryStatus.PENDING
    )
    attempts: Mapped[int] = mapped_column(default=0)
    scheduled_attempt: Mapped[int] = mapped_column(default=1)
    last_error: Mapped[str | None] = mapped_column(String(128))


class Outbox(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        Index(
            "ix_outbox_unpublished",
            "available_at",
            postgresql_where="published_at IS NULL",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    payment_id: Mapped[UUID] = mapped_column(ForeignKey("payments.id"))
    dedupe_key: Mapped[str] = mapped_column(String(100), unique=True)
    destination: Mapped[str] = mapped_column(String(32))
    payload: Mapped[dict] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
