from datetime import date, datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class MomentumRankingActionSummary(BaseModel):
    """Aggregate evaluation metrics for one momentum-ranking action."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    ranking_run_id: str = Field(min_length=1)
    input_dataset_version: str = Field(min_length=1)
    action: Literal["BUY", "HOLD", "SELL"]
    completed_count: int = Field(ge=0)
    average_forward_return: Decimal
    median_forward_return: Decimal
    directional_correct_count: int | None = Field(default=None, ge=0)
    directional_hit_rate: Decimal | None = Field(
        default=None,
        ge=Decimal(0),
        le=Decimal(1),
    )
    evaluation_as_of_date: date
    generated_at: datetime

    @field_validator("action", mode="before")
    @classmethod
    def normalize_action(cls, value: str) -> str:
        return value.strip().upper()

    @model_validator(mode="after")
    def validate_outcome_fields(self) -> "MomentumRankingActionSummary":

        if self.generated_at.tzinfo is None:
            raise ValueError("generated_at must include timezone information.")

        if self.evaluation_as_of_date > self.generated_at.date():
            raise ValueError("evaluation_as_of_date cannot be after generated_at.")

        if self.action == "HOLD":
            if (
                self.directional_correct_count is not None
                or self.directional_hit_rate is not None
            ):
                raise ValueError(
                    "HOLD summaries cannot include directional outcome fields."
                )
        else:
            if (
                self.directional_correct_count is None
                or self.directional_hit_rate is None
            ):
                raise ValueError(
                    "BUY and SELL summaries require directional outcome fields."
                )

            if self.directional_correct_count > self.completed_count:
                raise ValueError(
                    "directional_correct_count cannot exceed completed_count."
                )
        return self
