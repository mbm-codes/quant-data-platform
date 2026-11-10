from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class PipelineLogEvent(BaseModel):
    """One structured, append-only event emitted by a pipeline run."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    timestamp: datetime
    level: Literal["INFO", "WARNING", "ERROR"]
    event: Literal[
        "pipeline_started",
        "pipeline_completed",
        "pipeline_completed_with_warnings",
        "pipeline_failed",
    ]

    run_id: str = Field(min_length=1)
    pipeline_name: str = Field(min_length=1)
    status: Literal[
        "RUNNING",
        "COMPLETED",
        "COMPLETED_WITH_WARNINGS",
        "FAILED",
    ]

    files_discovered: int = Field(ge=0)
    files_processed: int = Field(ge=0)
    valid_rows: int = Field(ge=0)
    rejected_rows: int = Field(ge=0)
    error_message: str | None = None

    @model_validator(mode="after")
    def validate_event(self) -> "PipelineLogEvent":
        if self.timestamp.tzinfo is None:
            raise ValueError("timestamp must include timezone information.")

        expected_values = {
            "pipeline_started": ("INFO", "RUNNING"),
            "pipeline_completed": ("INFO", "COMPLETED"),
            "pipeline_completed_with_warnings": (
                "WARNING",
                "COMPLETED_WITH_WARNINGS",
            ),
            "pipeline_failed": ("ERROR", "FAILED"),
        }
        expected_level, expected_status = expected_values[self.event]

        if self.level != expected_level:
            raise ValueError(f"{self.event} events require level {expected_level}.")

        if self.status != expected_status:
            raise ValueError(f"{self.event} events require status {expected_status}.")

        if self.event == "pipeline_failed":
            if not self.error_message:
                raise ValueError("pipeline_failed events require error_message.")
        elif self.error_message is not None:
            raise ValueError(f"{self.event} events cannot include error_message.")

        return self
