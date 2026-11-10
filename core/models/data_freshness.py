from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class DataFreshnessResult(BaseModel):
    """Freshness assessment for one canonical dataset."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    dataset_name: str = Field(min_length=1)
    as_of_date: date
    latest_trade_date: date | None = None
    checked_at: datetime
    max_age_days: int = Field(ge=0)
    actual_age_days: int | None = Field(default=None, ge=0)
    status: Literal["FRESH", "STALE", "MISSING"]

    @model_validator(mode="after")
    def validate_result(self) -> "DataFreshnessResult":
        if self.checked_at.tzinfo is None:
            raise ValueError("checked_at must include timezone information.")

        if self.status == "MISSING":
            if (
                self.latest_trade_date is not None
                or self.actual_age_days is not None
            ):
                raise ValueError(
                    "MISSING freshness results cannot include date or age."
                )
            return self

        if (
            self.latest_trade_date is None
            or self.actual_age_days is None
        ):
            raise ValueError(
                "FRESH and STALE results require latest date and age."
            )

        if self.latest_trade_date > self.as_of_date:
            raise ValueError(
                "latest_trade_date cannot be after as_of_date."
            )

        if self.status == "FRESH":
            if self.actual_age_days > self.max_age_days:
                raise ValueError(
                    "FRESH results cannot exceed max_age_days."
                )

        if self.status == "STALE":
            if self.actual_age_days <= self.max_age_days:
                raise ValueError(
                    "STALE results must exceed max_age_days."
                )

        return self