# core/models/ranking.py

from datetime import date
from decimal import Decimal
from typing import Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)


class MomentumRankedCandidate(BaseModel):
    """One point-in-time momentum candidate ranked within an NSE universe."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    rank: int = Field(gt=0)
    exchange: Literal["NSE"] = "NSE"
    symbol: str = Field(min_length=1)
    as_of_date: date
    price_date: date
    reference_price: Decimal = Field(gt=0)

    momentum_return: Decimal
    score: Decimal = Field(ge=Decimal(0), le=Decimal(1))
    action: Literal["BUY", "HOLD", "SELL"]

    strategy_name: str = Field(min_length=1)
    strategy_version: str = Field(min_length=1)
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
    def validate_price_date(self) -> "MomentumRankedCandidate":
        if self.price_date > self.as_of_date:
            raise ValueError("price_date must be on or before as_of_date.")

        return self
