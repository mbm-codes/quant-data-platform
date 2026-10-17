from datetime import date
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field, field_validator


class PortfolioValuation(BaseModel):
    """One active position valued using a canonical daily close price."""

    account_id: str = Field(min_length=1)
    exchange: Literal["NSE"] = "NSE"
    symbol: str = Field(min_length=1)
    quantity: Decimal = Field(gt=0)

    price_date: date
    close_price: Decimal = Field(gt=0)
    market_value: Decimal = Field(gt=0)

    currency: Literal["INR"] = "INR"

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.strip().upper()
