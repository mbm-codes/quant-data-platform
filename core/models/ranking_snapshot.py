from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from core.models.ranking import MomentumRankedCandidate


class MomentumRankingRun(BaseModel):
    """Immutable metadata for one point-in-time momentum ranking run."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    ranking_run_id: str = Field(min_length=1)
    exchange: Literal["NSE"] = "NSE"
    as_of_date: date
    generated_at: datetime

    strategy_name: str = Field(min_length=1)
    strategy_version: str = Field(min_length=1)
    input_dataset_version: str = Field(min_length=1)

    universe_hash: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[0-9a-f]{64}$",
    )
    max_price_age_days: int = Field(ge=0)
    eligible_candidate_count: int = Field(ge=0)

    @field_validator("generated_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("generated_at must include timezone information.")

        return value


class MomentumRankingSnapshotCandidate(MomentumRankedCandidate):
    """One ranked candidate belonging to an immutable ranking run."""

    ranking_run_id: str = Field(min_length=1)
