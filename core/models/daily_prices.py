from datetime import date, datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator


class DailyPrice(BaseModel):
    """One canonical raw daily NSE equity price record."""

    exchange: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    trade_date: date

    open_price: Decimal = Field(gt=0)
    high_price: Decimal = Field(gt=0)
    low_price: Decimal = Field(gt=0)
    close_price: Decimal = Field(gt=0)
    volume_qty: int = Field(ge=0)

    currency: Literal["INR"] = "INR"
    price_basis: Literal["raw"] = "raw"

    source_name: str = Field(min_length=1)
    source_file: str = Field(min_length=1)
    source_hash: str | None = None
    ingested_at: datetime
    run_id: str = Field(min_length=1)
    dataset_version: str = Field(min_length=1)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.strip().upper()

    @model_validator(mode="after")
    def validate_ohlc_prices(self) -> "DailyPrice":
        if self.high_price < max(self.open_price, self.close_price, 
                                 self.low_price):
            raise ValueError("high_price must be at least open, close, and low.")

        if self.low_price > min(self.open_price, self.close_price, self.high_price):
            raise ValueError

        return self