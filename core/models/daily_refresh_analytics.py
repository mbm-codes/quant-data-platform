from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class DailyRefreshSummary(BaseModel):
    """Reliability summary across persisted daily-refresh outcomes."""

    model_config = ConfigDict(extra="forbid")

    total_refreshes: int = Field(ge=0)
    completed_count: int = Field(ge=0)
    completed_with_warnings_count: int = Field(ge=0)
    failed_count: int = Field(ge=0)
    success_rate: Decimal = Field(ge=Decimal(0), le=Decimal(1))
    latest_successful_finished_at: datetime | None = None
    generated_at: datetime

    @model_validator(mode="after")
    def validate_summary(self) -> "DailyRefreshSummary":
        if self.generated_at.tzinfo is None:
            raise ValueError("generated_at must include timezone information.")

        if (
            self.latest_successful_finished_at is not None
            and self.latest_successful_finished_at.tzinfo is None
        ):
            raise ValueError(
                "latest_successful_finished_at must include timezone information."
            )

        if (
            self.completed_count
            + self.completed_with_warnings_count
            + self.failed_count
            != self.total_refreshes
        ):
            raise ValueError("Outcome counts must add up to total_refreshes.")

        successful_count = (
            self.completed_count + self.completed_with_warnings_count
        )
        expected_success_rate = (
            Decimal(successful_count) / Decimal(self.total_refreshes)
            if self.total_refreshes > 0
            else Decimal(0)
        )

        if self.success_rate != expected_success_rate:
            raise ValueError(
                "success_rate must match the completed outcome counts."
            )

        if successful_count == 0 and self.latest_successful_finished_at is not None:
            raise ValueError(
                "No successful refreshes can include a latest successful time."
            )

        if successful_count > 0 and self.latest_successful_finished_at is None:
            raise ValueError(
                "Successful refreshes require a latest successful time."
            )

        return self