from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class DailyRefreshResult(BaseModel):
    """Final outcome of one daily NSE data refresh workflow."""

    model_config = ConfigDict(extra="forbid")

    run_id: str = Field(min_length=1)
    status: Literal["COMPLETED", "COMPLETED_WITH_WARNINGS", "FAILED"]

    started_at: datetime
    finished_at: datetime

    files_discovered: int = Field(ge=0)
    files_processed: int = Field(ge=0)
    files_failed: int = Field(ge=0)
    duplicate_files_skipped: int = Field(ge=0)
    valid_rows: int = Field(ge=0)
    rejected_rows: int = Field(ge=0)

    freshness_status: Literal["FRESH", "STALE", "MISSING"]
    quality_status: Literal["HEALTHY", "WARNING", "CRITICAL", "MISSING"]
    error_message: str | None = None

    @model_validator(mode="after")
    def validate_workflow_outcome(self) -> "DailyRefreshResult":
        if self.started_at.tzinfo is None or self.finished_at.tzinfo is None:
            raise ValueError(
                "started_at and finished_at must include timezone information."
            )

        if self.finished_at < self.started_at:
            raise ValueError("finished_at cannot be before started_at.")

        if self.files_processed + self.files_failed > self.files_discovered:
            raise ValueError(
                "processed and failed files cannot exceed discovered files."
            )

        if self.status == "FAILED" and not self.error_message:
            raise ValueError("Failed refreshes require an error_message.")

        if self.status != "FAILED" and self.error_message is not None:
            raise ValueError("Only failed refreshes can include an error_message.")

        return self
