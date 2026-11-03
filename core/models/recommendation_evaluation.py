from datetime import date
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field, model_validator


class RecommendationEvaluation(BaseModel):
    """Outcome of evaluating one recorded recommendation."""

    recommendation_id: str = Field(min_length=1)
    evaluation_as_of_date: date
    future_observations_found: int = Field(ge=0)

    status: Literal["PENDING", "COMPLETED"]

    future_price_date: date | None = None
    future_close_price: Decimal | None = Field(default=None, gt=0)
    forward_return: Decimal | None = None

    @model_validator(mode="after")
    def validate_status_fields(self) -> "RecommendationEvaluation":
        completed_fields = (
            self.future_price_date,
            self.future_close_price,
            self.forward_return,
        )

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
