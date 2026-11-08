from datetime import date
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class MomentumRankingCandidateEvaluation(BaseModel):
    """Evaluation outcome for one candidate from a recorded ranking run."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    ranking_run_id: str = Field(min_length=1)
    exchange: Literal["NSE"] = "NSE"
    symbol: str = Field(min_length=1)
    rank: int = Field(gt=0)
    action: Literal["BUY", "HOLD", "SELL"]

    price_date: date
    reference_price: Decimal = Field(gt=0)
    input_dataset_version: str = Field(min_length=1)

    evaluation_as_of_date: date
    future_observations_found: int = Field(ge=0)
    status: Literal["PENDING", "COMPLETED"]

    future_price_date: date | None = None
    future_close_price: Decimal | None = Field(default=None, gt=0)
    forward_return: Decimal | None = None

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.upper()

    @field_validator("action", mode="before")
    @classmethod
    def normalize_action(cls, value: str) -> str:
        return value.strip().upper()

    @model_validator(mode="after")
    def validate_outcome_fields(self) -> "MomentumRankingCandidateEvaluation":
        completed_fields = (
            self.future_price_date,
            self.future_close_price,
            self.forward_return,
        )

        if self.evaluation_as_of_date < self.price_date:
            raise ValueError("evaluation_as_of_date cannot be before price_date.")

        if self.status == "COMPLETED":
            if self.future_observations_found < 20:
                raise ValueError(
                    "Completed evaluations require at least 20 future observations."
                )

            if any(value is None for value in completed_fields):
                raise ValueError(
                    "Completed evaluations require future price and return fields."
                )

        if self.status == "PENDING" and any(
            value is not None for value in completed_fields
        ):
            raise ValueError(
                "Pending evaluations cannot include future price or return fields."
            )

        return self
