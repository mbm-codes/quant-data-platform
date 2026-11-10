from datetime import date, datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class MomentumRankingBacktestSummary(BaseModel):
    """Aggregate performance of a momentum ranking across historical dates."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    exchange: Literal["NSE"] = "NSE"
    strategy_name: str = Field(min_length=1)
    strategy_version: str = Field(min_length=1)
    input_dataset_version: str = Field(min_length=1)
    universe_hash: str = Field(
        pattern=r"^[a-f0-9]{64}$",
    )

    start_date: date
    end_date: date
    bucket_size: int = Field(gt=0)
    step_observations: int = Field(gt=0)
    forward_horizon_observations: int = Field(gt=0)
    attempted_ranking_date_count: int = Field(gt=0)
    completed_ranking_date_count: int = Field(gt=0)
    positive_rank_spread_count: int = Field(ge=0)
    rank_spread_hit_rate: Decimal = Field(
        ge=Decimal(0),
        le=Decimal(1),
    )
    average_rank_spread: Decimal
    median_rank_spread: Decimal
    generated_at: datetime

    @model_validator(mode="after")
    def validate_summary(self) -> "MomentumRankingBacktestSummary":
        if self.generated_at.tzinfo is None:
            raise ValueError("generated_at must include timezone information.")

        if self.start_date > self.end_date:
            raise ValueError("start_date cannot be after end_date.")

        if self.end_date > self.generated_at.date():
            raise ValueError("end_date cannot be after generated_at.")

        if self.positive_rank_spread_count > self.completed_ranking_date_count:
            raise ValueError(
                "positive_rank_spread_count cannot exceed completed_ranking_date_count."
            )

        if self.completed_ranking_date_count > self.attempted_ranking_date_count:
            raise ValueError(
                "completed_ranking_date_count cannot exceed "
                "attempted_ranking_date_count."
            )

        return self


class MomentumRankingBacktestObservation(BaseModel):
    """Top-versus-bottom bucket outcome for one historical ranking date."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    exchange: Literal["NSE"] = "NSE"
    strategy_name: str = Field(min_length=1)
    strategy_version: str = Field(min_length=1)
    input_dataset_version: str = Field(min_length=1)
    universe_hash: str = Field(
        pattern=r"^[a-f0-9]{64}$",
    )

    ranking_date: date
    evaluation_as_of_date: date
    bucket_size: int = Field(gt=0)

    top_rank_start: int = Field(gt=0)
    top_rank_end: int = Field(gt=0)
    bottom_rank_start: int = Field(gt=0)
    bottom_rank_end: int = Field(gt=0)

    top_completed_count: int = Field(gt=0)
    bottom_completed_count: int = Field(gt=0)
    top_average_forward_return: Decimal
    bottom_average_forward_return: Decimal
    rank_spread: Decimal
    top_positive_return_rate: Decimal = Field(
        ge=Decimal(0),
        le=Decimal(1),
    )
    bottom_positive_return_rate: Decimal = Field(
        ge=Decimal(0),
        le=Decimal(1),
    )
    generated_at: datetime

    @model_validator(mode="after")
    def validate_observation(self) -> "MomentumRankingBacktestObservation":
        if self.generated_at.tzinfo is None:
            raise ValueError("generated_at must include timezone information.")

        if self.ranking_date >= self.evaluation_as_of_date:
            raise ValueError("evaluation_as_of_date must be after ranking_date.")

        if self.evaluation_as_of_date > self.generated_at.date():
            raise ValueError("evaluation_as_of_date cannot be after generated_at.")

        if self.top_rank_start > self.top_rank_end:
            raise ValueError("Top rank range is invalid.")

        if self.bottom_rank_start > self.bottom_rank_end:
            raise ValueError("Bottom rank range is invalid.")

        if self.top_rank_end >= self.bottom_rank_start:
            raise ValueError("Top and bottom rank ranges cannot overlap.")

        if self.top_completed_count > self.bucket_size:
            raise ValueError("top_completed_count cannot exceed bucket_size.")

        if self.bottom_completed_count > self.bucket_size:
            raise ValueError("bottom_completed_count cannot exceed bucket_size.")

        expected_rank_spread = (
            self.top_average_forward_return - self.bottom_average_forward_return
        )
        if self.rank_spread != expected_rank_spread:
            raise ValueError(
                "rank_spread must equal top average return minus bottom average return."
            )

        return self
