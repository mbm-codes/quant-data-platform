from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field, field_validator


class PortfolioPosition(BaseModel):
    """An active long position calculated from transaction events."""

    account_id: str = Field(min_length=1)
    exchange: Literal["NSE"] = "NSE"
    symbol: str = Field(min_length=1)
    quantity: Decimal = Field(gt=0)
    currency: Literal["INR"] = "INR"

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.strip().upper()
