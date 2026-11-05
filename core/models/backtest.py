# core/models/backtest.py

from datetime import date
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

BacktestAction = Literal["BUY", "HOLD", "SELL"]


class MomentumBacktestObservation(BaseModel):
    """One completed historical momentum-strategy observation."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    exchange: Literal["NSE"] = "NSE"
    symbol: str = Field(min_length=1)
    as_of_date: date
    reference_price: Decimal = Field(gt=0)

    momentum_return: Decimal
    score: Decimal = Field(ge=Decimal(0), le=Decimal(1))
    action: Literal["BUY", "HOLD", "SELL"]

    future_price_date: date
    future_close_price: Decimal = Field(gt=0)
    forward_return: Decimal

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
    def validate_dates(self) -> "MomentumBacktestObservation":
        if self.future_price_date <= self.as_of_date:
            raise ValueError("future_price_date must be after as_of_date.")

        return self


class MomentumBacktestActionSummary(BaseModel):
    """Action-level outcome metrics for a momentum backtest."""

    action: Literal["BUY", "HOLD", "SELL"]
    observation_count: int = Field(gt=0)
    average_forward_return: Decimal

    directional_observations: int = Field(ge=0)
    directional_correct_count: int = Field(ge=0)
    directional_hit_rate: Decimal | None = None

    median_forward_return: Decimal
