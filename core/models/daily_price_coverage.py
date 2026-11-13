from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class DailyPriceCoverage(BaseModel):
    """Read-only coverage report for one canonical daily-price dataset."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    exchange: Literal["NSE"] = "NSE"
    dataset_version: str = Field(min_length=1)
    total_rows: int = Field(ge=0)
    distinct_symbols: int = Field(ge=0)
    first_trade_date: date | None = None
    last_trade_date: date | None = None
    duplicate_business_key_count: int = Field(ge=0)
    generated_at: datetime

    @model_validator(mode="after")
    def validate_coverage(self) -> "DailyPriceCoverage":
        if self.generated_at.tzinfo is None:
            raise ValueError("generated_at must include timezone information.")

        if self.distinct_symbols > self.total_rows:
            raise ValueError("distinct_symbols cannot exceed total_rows.")

        if self.duplicate_business_key_count > self.total_rows:
            raise ValueError(
                "duplicate_business_key_count cannot exceed total_rows."
            )

        if self.total_rows == 0:
            if (
                self.distinct_symbols != 0
                or self.first_trade_date is not None
                or self.last_trade_date is not None
            ):
                raise ValueError(
                    "Empty coverage must have zero symbols and no trade dates."
                )

            return self

        if self.first_trade_date is None or self.last_trade_date is None:
            raise ValueError(
                "Non-empty coverage requires first and last trade dates."
            )

        if self.first_trade_date > self.last_trade_date:
            raise ValueError(
                "first_trade_date cannot be after last_trade_date."
            )

        return self