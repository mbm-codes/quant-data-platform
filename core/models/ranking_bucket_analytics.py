from datetime import date, datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class MomentumRankingBucketSummary(BaseModel):
    """Aggregate outcome metrics for a top or bottom ranking bucket."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    ranking_run_id: str = Field(min_length=1)
    input_dataset_version: str = Field(min_length=1)
    bucket: Literal["TOP", "BOTTOM"]
    bucket_size: int = Field(gt=0)
    rank_start: int = Field(gt=0)
    rank_end: int = Field(gt=0)
    completed_count: int = Field(gt=0)
    average_forward_return: Decimal
    median_forward_return: Decimal
    positive_return_count: int = Field(ge=0)
    positive_return_rate: Decimal = Field(
        ge=Decimal(0),
        le=Decimal(1),
    )
    evaluation_as_of_date: date
    generated_at: datetime

    @field_validator("bucket", mode="before")
    @classmethod
    def normalize_bucket(cls, value: str) -> str:
        return value.strip().upper()

    @model_validator(mode="after")
    def validate_summary(self) -> "MomentumRankingBucketSummary":
        if self.generated_at.tzinfo is None:
            raise ValueError("generated_at must include timezone information.")

        if self.evaluation_as_of_date > self.generated_at.date():
            raise ValueError(
                "evaluation_as_of_date cannot be after generated_at."
            )

        if self.rank_start > self.rank_end:
            raise ValueError("rank_start cannot be after rank_end.")

        if self.completed_count > self.bucket_size:
            raise ValueError("completed_count cannot exceed bucket_size.")

        if self.positive_return_count > self.completed_count:
            raise ValueError(
                "positive_return_count cannot exceed completed_count."
            )

        return self