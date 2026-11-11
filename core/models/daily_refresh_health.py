from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

DailyRefreshOutcomeStatus = Literal[
    "COMPLETED",
    "COMPLETED_WITH_WARNINGS",
    "FAILED",
]


class DailyRefreshHealth(BaseModel):
    """Actionable health decision derived from daily-refresh history."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    status: Literal["HEALTHY", "WARNING", "CRITICAL", "MISSING"]
    latest_run_id: str | None = Field(default=None, min_length=1)
    latest_refresh_status: DailyRefreshOutcomeStatus | None = None
    latest_finished_at: datetime | None = None
    consecutive_failure_count: int = Field(ge=0)
    max_consecutive_failures: int = Field(gt=0)
    reason: str = Field(min_length=1)
    checked_at: datetime

    @model_validator(mode="after")
    def validate_health_state(self) -> "DailyRefreshHealth":
        if self.checked_at.tzinfo is None:
            raise ValueError("checked_at must include timezone information.")

        if (
            self.latest_finished_at is not None
            and self.latest_finished_at.tzinfo is None
        ):
            raise ValueError(
                "latest_finished_at must include timezone information."
            )

        latest_fields = (
            self.latest_run_id,
            self.latest_refresh_status,
            self.latest_finished_at,
        )

        if self.status == "MISSING":
            if any(value is not None for value in latest_fields):
                raise ValueError(
                    "MISSING health cannot include latest refresh fields."
                )

            if self.consecutive_failure_count != 0:
                raise ValueError(
                    "MISSING health must have zero consecutive failures."
                )

            return self

        if (
            self.latest_run_id is None
            or self.latest_refresh_status is None
            or self.latest_finished_at is None
        ):
            raise ValueError(
                "Non-missing health requires latest refresh fields."
            )

        if self.latest_finished_at > self.checked_at:
            raise ValueError(
                "latest_finished_at cannot be after checked_at."
            )

        if (
            self.latest_refresh_status == "FAILED"
            and self.consecutive_failure_count == 0
        ):
            raise ValueError(
                "A failed latest refresh requires consecutive failures."
            )

        if (
            self.latest_refresh_status != "FAILED"
            and self.consecutive_failure_count != 0
        ):
            raise ValueError(
                "Only a failed latest refresh can have consecutive failures."
            )

        is_critical = (
            self.consecutive_failure_count
            >= self.max_consecutive_failures
        )

        if self.status == "CRITICAL" and not is_critical:
            raise ValueError(
                "CRITICAL health requires a failed latest refresh or "
                "the configured consecutive-failure limit."
            )

        if self.status == "HEALTHY" and (
            self.latest_refresh_status != "COMPLETED"
            or self.consecutive_failure_count != 0
        ):
            raise ValueError(
                "HEALTHY health requires a completed latest refresh "
                "and zero consecutive failures."
            )

        if self.status == "WARNING" and (
            is_critical
            or self.latest_refresh_status == "COMPLETED"
        ):
            raise ValueError(
                "WARNING health requires a warning or failed latest refresh "
                "below the configured consecutive-failure limit."
            )

        return self