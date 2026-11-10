"""Create payments and transactional outbox"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "9915aca3312e"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "payments",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("amount", sa.Numeric(precision=18, scale=2), nullable=False),
        sa.Column("currency", sa.String(length=3), nullable=False),
        sa.Column("description", sa.String(length=1000), nullable=False),
        sa.Column(
            "metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False
        ),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("idempotency_key", sa.String(length=128), nullable=False),
        sa.Column("request_hash", sa.String(length=64), nullable=False),
        sa.Column("webhook_url", sa.String(length=2048), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("webhook_event_id", sa.Uuid(), nullable=False),
        sa.Column(
            "webhook_delivered_at", sa.DateTime(timezone=True), nullable=True
        ),
        sa.Column("delivery_status", sa.String(length=16), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("scheduled_attempt", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.String(length=128), nullable=True),
        sa.CheckConstraint(
            "currency IN ('RUB', 'USD', 'EUR')", name="payment_currency"
        ),
        sa.CheckConstraint(
            "delivery_status IN ('pending', 'delivered', 'dead')",
            name="delivery_status",
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'succeeded', 'failed')",
            name="payment_status",
        ),
        sa.CheckConstraint("amount > 0", name="payment_amount_positive"),
        sa.CheckConstraint(
            "attempts BETWEEN 0 AND 3", name="payment_attempts"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key"),
        sa.UniqueConstraint("webhook_event_id"),
    )
    op.create_table(
        "outbox",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("payment_id", sa.Uuid(), nullable=False),
        sa.Column("dedupe_key", sa.String(length=100), nullable=False),
        sa.Column("destination", sa.String(length=32), nullable=False),
        sa.Column(
            "payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["payment_id"],
            ["payments.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("dedupe_key"),
    )
    op.create_index(
        "ix_outbox_unpublished",
        "outbox",
        ["available_at"],
        unique=False,
        postgresql_where="published_at IS NULL",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_outbox_unpublished",
        table_name="outbox",
        postgresql_where="published_at IS NULL",
    )
    op.drop_table("outbox")
    op.drop_table("payments")
