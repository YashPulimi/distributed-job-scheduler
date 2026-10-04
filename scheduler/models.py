from datetime import datetime, timezone
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt, field_validator


class Payload(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SumPayload(Payload):
    numbers: list[Annotated[StrictInt, Field(ge=-1_000_000_000, le=1_000_000_000)]] = Field(
        min_length=1, max_length=10_000
    )


class TextPayload(Payload):
    text: str = Field(max_length=100_000)


class DemoPayload(Payload):
    duration_seconds: float = Field(default=0.1, ge=0, le=30, allow_inf_nan=False)
    fail_first_attempts: int = Field(default=0, ge=0, le=5, strict=True)


PAYLOAD_TYPES = {"sum": SumPayload, "word_count": TextPayload, "demo": DemoPayload}


class JobRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["sum", "word_count", "demo"]
    payload: dict
    priority: int = Field(default=5, ge=0, le=10, strict=True)
    max_attempts: int = Field(default=3, ge=1, le=10, strict=True)
    scheduled_at: datetime | None = None

    @field_validator("scheduled_at")
    @classmethod
    def require_timezone(cls, value):
        if value is not None:
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError("scheduled_at must include a timezone")
            return value.astimezone(timezone.utc)
        return value

