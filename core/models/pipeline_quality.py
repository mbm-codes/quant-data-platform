from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

PipelineRunStatus = Literal[
    "RUNNING",
    "COMPLETED",
    "COMPLETED_WITH_WARNINGS",
    "FAILED",
]


class PipelineQualityResult(BaseModel):
    """Quality assessment derived from the latest pipeline-run record."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    pipeline_name: str = Field(min_length=1)
    latest_run_id: str | None = None
    latest_run_status: PipelineRunStatus | None = None

    files_discovered: int | None = Field(default=None, ge=0)
    files_processed: int | None = Field(default=None, ge=0)
    valid_rows: int | None = Field(default=None, ge=0)
    rejected_rows: int | None = Field(default=None, ge=0)
    rejection_rate: Decimal | None = Field(
        default=None,
        ge=Decimal(0),
        le=Decimal(1),
    )

    warning_rejection_rate: Decimal = Field(
        ge=Decimal(0),
        le=Decimal(1),
    )
    critical_rejection_rate: Decimal = Field(
        ge=Decimal(0),
        le=Decimal(1),
    )
    status: Literal["HEALTHY", "WARNING", "CRITICAL", "MISSING"]
    checked_at: datetime

    @model_validator(mode="after")
    def validate_result(self) -> "PipelineQualityResult":
        if self.checked_at.tzinfo is None:
            raise ValueError("checked_at must include timezone information.")

        if self.warning_rejection_rate > self.critical_rejection_rate:
            raise ValueError(
                "warning_rejection_rate cannot exceed "
                "critical_rejection_rate."
            )

        metric_fields = (
            self.files_discovered,
            self.files_processed,
            self.valid_rows,
            self.rejected_rows,
            self.rejection_rate,
        )

        if self.status == "MISSING":
            if (
                self.latest_run_id is not None
                or self.latest_run_status is not None
                or any(value is not None for value in metric_fields)
            ):
                raise ValueError(
                    "MISSING quality results cannot include run data."
                )
            return self

        if (
            self.latest_run_id is None
            or self.latest_run_status is None
            or self.files_discovered is None
            or self.files_processed is None
            or self.valid_rows is None
            or self.rejected_rows is None
            or self.rejection_rate is None
        ):
            raise ValueError(
                "Non-missing quality results require run data and metrics."
            )

        if self.files_processed > self.files_discovered:
            raise ValueError(
                "files_processed cannot exceed files_discovered."
            )

        expected_status = _expected_quality_status(
            self.latest_run_status,
            self.rejection_rate,
            self.warning_rejection_rate,
            self.critical_rejection_rate,
        )
        if self.status != expected_status:
            raise ValueError(
                f"Quality status must be {expected_status} for "
                "the supplied run data."
            )

        return self


def _expected_quality_status(
    pipeline_status: PipelineRunStatus,
    rejection_rate: Decimal,
    warning_rejection_rate: Decimal,
    critical_rejection_rate: Decimal,
) -> Literal["HEALTHY", "WARNING", "CRITICAL"]:
    if (
        pipeline_status == "FAILED"
        or rejection_rate > critical_rejection_rate
    ):
        return "CRITICAL"

    if (
        pipeline_status in {"RUNNING", "COMPLETED_WITH_WARNINGS"}
        or rejection_rate > warning_rejection_rate
    ):
        return "WARNING"

    return "HEALTHY"