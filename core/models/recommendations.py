from datetime import date, datetime
from decimal import Decimal
from typing import Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)


class PortfolioRecommendation(BaseModel):
    """One versioned, auditable portfolio recommendation."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    recommendation_id: str = Field(min_length=1)
    account_id: str = Field(min_length=1)

    exchange: Literal["NSE"] = "NSE"
    symbol: str = Field(min_length=1)
    action: Literal["BUY", "HOLD", "SELL"]
    as_of_date: date
    price_date: date
    reference_price: Decimal = Field(gt=0)
    currency: Literal["INR"] = "INR"
    score: Decimal = Field(ge=Decimal(0), le=Decimal(1))
    rationale: str = Field(min_length=1)
    strategy_name: str = Field(min_length=1)
    strategy_version: str = Field(min_length=1)
    generated_at: datetime
    run_id: str = Field(min_length=1)
    input_dataset_version: str = Field(min_length=1)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.upper()

    @field_validator("action", mode="before")
    @classmethod
    def normalize_action(cls, value: str) -> str:
        return value.strip().upper()

    @model_validator(mode="after")
    def validate_dates(self) -> "PortfolioRecommendation":
        if self.price_date > self.as_of_date:
            raise ValueError("price_date must be on or before as_of_date.")

        if self.generated_at.tzinfo is None:
            raise ValueError("generated_at must include timezone information.")

        return self
