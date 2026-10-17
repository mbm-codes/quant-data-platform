from datetime import date, datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field, field_validator


class PortfolioTransaction(BaseModel):
    """One immutable buy or sell event for a portfolio account."""

    transaction_id: str = Field(min_length=1)
    account_id: str = Field(min_length=1)

    exchange: Literal["NSE"] = "NSE"
    symbol: str = Field(min_length=1)
    trade_date: date
    side: Literal["BUY", "SELL"]

    quantity: Decimal = Field(gt=0)
    unit_price: Decimal = Field(gt=0)
    fees: Decimal = Field(ge=0)
    currency: Literal["INR"] = "INR"

    source_file: str = Field(min_length=1)
    ingested_at: datetime
    run_id: str = Field(min_length=1)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator("side", mode="before")
    @classmethod
    def normalize_side(cls, value: str) -> str:
        return value.strip().upper()
