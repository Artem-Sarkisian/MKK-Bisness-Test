from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from mkk_bisness_test.database import Database
from mkk_bisness_test.domain import IdempotencyConflict
from mkk_bisness_test.models import Outbox, Payment
from mkk_bisness_test.schemas import PaymentCreate


class PaymentService:
    def __init__(self, database: Database) -> None:
        self.database = database

    async def create(self, request: PaymentCreate, key: str) -> Payment:
        fingerprint = request.fingerprint()
        payment_id = uuid4()
        async with self.database.sessions.begin() as session:
            statement = (
                insert(Payment)
                .values(
                    id=payment_id,
                    amount=request.amount,
                    currency=request.currency,
                    description=request.description,
                    metadata_=request.metadata,
                    webhook_url=str(request.webhook_url),
                    idempotency_key=key,
                    request_hash=fingerprint,
                )
                .on_conflict_do_nothing(
                    index_elements=[Payment.idempotency_key]
                )
                .returning(Payment)
            )
            payment = (await session.execute(statement)).scalar_one_or_none()
            if payment is None:
                payment = (
                    await session.execute(
                        select(Payment).where(Payment.idempotency_key == key)
                    )
                ).scalar_one()
                if payment.request_hash != fingerprint:
                    raise IdempotencyConflict
                return payment
            session.add(
                Outbox(
                    payment_id=payment.id,
                    dedupe_key=f"{payment.id}:1",
                    destination="payments.new",
                    payload={"payment_id": str(payment.id), "attempt": 1},
                )
            )
            return payment

    async def get(self, payment_id: UUID) -> Payment | None:
        async with self.database.sessions() as session:
            return await session.get(Payment, payment_id)
