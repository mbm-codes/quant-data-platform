from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class PipelineRun(BaseModel):
    """Operational record for one execution of a data pipeline."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    # Identity
    run_id: str = Field(min_length=1)
    pipeline_name: str = Field(min_length=1)
    input_dataset_version: str = Field(min_length=1)

    # Lifecycle
    status: Literal["RUNNING", "COMPLETED", "COMPLETED_WITH_WARNINGS", "FAILED"]
    started_at: datetime
    finished_at: datetime | None = None

    # Operational counts
    files_discovered: int = Field(ge=0)
    files_processed: int = Field(ge=0)
    valid_rows: int = Field(ge=0)
    rejected_rows: int = Field(ge=0)

    # Failure details
    error_message: str | None = None

    @model_validator(mode="after")
    def validate_lifecycle(self) -> "PipelineRun":
        if self.started_at.tzinfo is None:
            raise ValueError("started_at must include timezone information.")

        if self.finished_at is not None:
            if self.finished_at.tzinfo is None:
                raise ValueError(
                    "finished_at must include timezone information."
                )

            if self.finished_at < self.started_at:
                raise ValueError(
                    "finished_at cannot be before started_at."
                )

        if self.status == "RUNNING":
            if self.finished_at is not None:
                raise ValueError(
                    "RUNNING pipeline runs cannot include finished_at."
                )

            if self.error_message is not None:
                raise ValueError(
                    "RUNNING pipeline runs cannot include error_message."
                )

        if self.status in {"COMPLETED", "COMPLETED_WITH_WARNINGS"}:
            if self.finished_at is None:
                raise ValueError(
                    f"{self.status} pipeline runs require finished_at."
                )

            if self.error_message is not None:
                raise ValueError(
                    f"{self.status} pipeline runs cannot include "
                    "error_message."
                )
        if self.status == "FAILED":
            if self.finished_at is None:
                raise ValueError(
                    "FAILED pipeline runs require finished_at."
                )

            if not self.error_message:
                raise ValueError(
                    "FAILED pipeline runs require error_message."
                )

        return self