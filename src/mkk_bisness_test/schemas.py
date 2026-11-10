import hashlib
import json
import math
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    JsonValue,
    field_validator,
)

from mkk_bisness_test.domain import PaymentStatus


def validate_text(value: str) -> str:
    if "\x00" in value:
        raise ValueError("NUL characters are not supported")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        raise ValueError("Text must contain valid Unicode") from None
    return value


def validate_json(value: JsonValue, depth: int = 0) -> None:
    if depth > 32:
        raise ValueError("metadata nesting must not exceed 32 levels")
    if isinstance(value, str):
        validate_text(value)
    elif isinstance(value, float) and not math.isfinite(value):
        raise ValueError("JSON numbers must be finite")
    elif isinstance(value, dict):
        for key, item in value.items():
            validate_text(key)
            validate_json(item, depth + 1)
    elif isinstance(value, list):
        for item in value:
            validate_json(item, depth + 1)


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]
    currency: Literal["RUB", "USD", "EUR"]
    description: str = Field(max_length=1000)
    metadata: dict[str, JsonValue]
    webhook_url: HttpUrl = Field(max_length=2048)

    @field_validator("description")
    @classmethod
    def valid_description(cls, value: str) -> str:
        return validate_text(value)

    @field_validator("metadata")
    @classmethod
    def limit_metadata(cls, value: dict) -> dict:
        validate_json(value)
        if (
            len(
                json.dumps(value, ensure_ascii=False, allow_nan=False).encode()
            )
            > 16384
        ):
            raise ValueError("metadata must not exceed 16 KiB")
        return value

    @field_validator("webhook_url")
    @classmethod
    def disallow_credentials(cls, value: HttpUrl) -> HttpUrl:
        if value.username or value.password or value.fragment:
            raise ValueError(
                "webhook URL must not contain credentials or a fragment"
            )
        return value

    def fingerprint(self) -> str:
        data = self.model_dump(mode="json")
        data["amount"] = format(self.amount, ".2f")
        encoded = json.dumps(
            data, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
        return hashlib.sha256(encoded.encode()).hexdigest()


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class PaymentDetails(PaymentAccepted):
    amount: Decimal
    currency: str
    description: str
    metadata: dict[str, JsonValue]
    idempotency_key: str
    webhook_url: str
    processed_at: datetime | None


class PaymentJob(BaseModel):
    model_config = ConfigDict(extra="forbid")

    payment_id: UUID
    attempt: int = Field(ge=1, le=3, strict=True)
