import logging
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError

from mkk_bisness_test.config import Settings, get_settings
from mkk_bisness_test.database import Database
from mkk_bisness_test.domain import IdempotencyConflict
from mkk_bisness_test.ingress import RequestGuard
from mkk_bisness_test.payments import PaymentService
from mkk_bisness_test.schemas import (
    PaymentAccepted,
    PaymentCreate,
    PaymentDetails,
)

logger = logging.getLogger(__name__)


def get_service(request: Request) -> PaymentService:
    return request.app.state.payments


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    database = Database(settings.database_url)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        await database.close()

    app = FastAPI(
        title="Payment processing",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.add_middleware(
        RequestGuard, api_key=settings.api_key.get_secret_value()
    )
    app.state.payments = PaymentService(database)

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request: Request, error: RequestValidationError):
        def safe_text(value: object) -> str:
            return (
                str(value).encode("utf-8", "backslashreplace").decode()[:200]
            )

        details = [
            {
                "type": item["type"],
                "loc": [
                    part if isinstance(part, int) else safe_text(part)
                    for part in item["loc"][:10]
                ],
                "msg": safe_text(item["msg"]),
            }
            for item in error.errors()[:20]
        ]
        return JSONResponse({"detail": details}, status_code=422)

    @app.exception_handler(SQLAlchemyError)
    @app.exception_handler(OSError)
    async def storage_error(request: Request, error: Exception):
        logger.error("storage_unavailable error_type=%s", type(error).__name__)
        return JSONResponse(
            {"detail": "Storage temporarily unavailable"},
            status_code=503,
            headers={"Retry-After": "1"},
        )

    @app.post(
        "/api/v1/payments", status_code=202, response_model=PaymentAccepted
    )
    async def create_payment(
        body: PaymentCreate,
        idempotency_key: Annotated[
            str,
            Header(
                min_length=1, max_length=128, pattern=r"^[A-Za-z0-9._:-]+$"
            ),
        ],
        service: Annotated[PaymentService, Depends(get_service)],
    ) -> PaymentAccepted:
        try:
            payment = await service.create(body, idempotency_key)
        except IdempotencyConflict:
            raise HTTPException(
                status_code=409,
                detail="Idempotency key already used with different data",
            ) from None
        return PaymentAccepted(
            payment_id=payment.id,
            status=payment.status,
            created_at=payment.created_at,
        )

    @app.get("/api/v1/payments/{payment_id}", response_model=PaymentDetails)
    async def get_payment(
        payment_id: UUID,
        service: Annotated[PaymentService, Depends(get_service)],
    ) -> PaymentDetails:
        payment = await service.get(payment_id)
        if payment is None:
            raise HTTPException(status_code=404, detail="Payment not found")
        return PaymentDetails(
            payment_id=payment.id,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.metadata_,
            status=payment.status,
            idempotency_key=payment.idempotency_key,
            webhook_url=payment.webhook_url,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
        )

    return app
