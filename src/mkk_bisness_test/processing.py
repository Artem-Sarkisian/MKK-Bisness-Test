import logging
from datetime import timedelta

from sqlalchemy.ext.asyncio import AsyncSession

from mkk_bisness_test.database import Database
from mkk_bisness_test.domain import (
    DeliveryError,
    DeliveryStatus,
    Gateway,
    PaymentStatus,
    WebhookSender,
    utcnow,
)
from mkk_bisness_test.models import Outbox, Payment
from mkk_bisness_test.schemas import PaymentJob

logger = logging.getLogger(__name__)
MAX_ATTEMPTS = 3


class PaymentProcessor:
    def __init__(
        self,
        database: Database,
        gateway: Gateway,
        sender: WebhookSender,
        retry_base: float,
    ) -> None:
        self.database = database
        self.gateway = gateway
        self.sender = sender
        self.retry_base = retry_base

    async def process(self, job: PaymentJob) -> None:
        async with self.database.payment_lock(job.payment_id):
            async with self.database.sessions.begin() as session:
                payment = await session.get(Payment, job.payment_id)
                if payment is None:
                    raise ValueError("unknown_payment")
                if (
                    payment.delivery_status != DeliveryStatus.PENDING
                    or job.attempt < payment.scheduled_attempt
                ):
                    return
                if job.attempt > payment.scheduled_attempt:
                    raise ValueError("unexpected_attempt")
                if payment.attempts >= job.attempt:
                    # The receiver may have accepted the request already.
                    # Count interrupted attempts to keep retries bounded.
                    self._retry_or_dead(
                        session, payment, job, "interrupted_attempt"
                    )
                    return
                payment.attempts = job.attempt

            try:
                if payment.status == PaymentStatus.PENDING:
                    succeeded = await self.gateway.charge(
                        payment.id, payment.amount, payment.currency
                    )
                    async with self.database.sessions.begin() as session:
                        payment = await session.get(Payment, job.payment_id)
                        payment.status = (
                            PaymentStatus.SUCCEEDED
                            if succeeded
                            else PaymentStatus.FAILED
                        )
                        payment.processed_at = utcnow()
                await self.sender.send(
                    payment.webhook_url, self._payload(payment)
                )
            except Exception as error:
                code = (
                    str(error)
                    if isinstance(error, DeliveryError)
                    else "processing_error"
                )
                async with self.database.sessions.begin() as session:
                    payment = await session.get(Payment, job.payment_id)
                    self._retry_or_dead(session, payment, job, code)
                logger.warning(
                    "payment=%s attempt=%s error=%s",
                    job.payment_id,
                    job.attempt,
                    code,
                )
                return

            async with self.database.sessions.begin() as session:
                payment = await session.get(Payment, job.payment_id)
                payment.delivery_status = DeliveryStatus.DELIVERED
                payment.webhook_delivered_at = utcnow()
                payment.last_error = None
            logger.info(
                "payment=%s attempt=%s webhook_delivered",
                job.payment_id,
                job.attempt,
            )

    def _retry_or_dead(
        self,
        session: AsyncSession,
        payment: Payment,
        job: PaymentJob,
        error: str,
    ) -> None:
        payment.last_error = error
        if job.attempt >= MAX_ATTEMPTS:
            payment.delivery_status = DeliveryStatus.DEAD
            destination = "payments.dlq"
            key = f"{payment.id}:dead"
            payload = {**job.model_dump(mode="json"), "reason": error}
            available_at = utcnow()
        else:
            payment.scheduled_attempt = job.attempt + 1
            destination = "payments.new"
            key = f"{payment.id}:{payment.scheduled_attempt}"
            payload = {
                "payment_id": str(payment.id),
                "attempt": payment.scheduled_attempt,
            }
            available_at = utcnow() + timedelta(
                seconds=self.retry_base * 2 ** (job.attempt - 1)
            )
        session.add(
            Outbox(
                payment_id=payment.id,
                dedupe_key=key,
                destination=destination,
                payload=payload,
                available_at=available_at,
            )
        )

    @staticmethod
    def _payload(payment: Payment) -> dict:
        return {
            "event_id": str(payment.webhook_event_id),
            "event_type": "payment.completed",
            "payment_id": str(payment.id),
            "status": payment.status,
            "amount": format(payment.amount, ".2f"),
            "currency": payment.currency,
            "processed_at": payment.processed_at.isoformat(),
        }
