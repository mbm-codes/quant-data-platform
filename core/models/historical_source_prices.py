from datetime import date, datetime
from decimal import Decimal
from typing import Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, Field, field_validator, model_validator

IST = ZoneInfo("Asia/Kolkata")

class HistoricalSourcePrice(BaseModel):
    """A validated record from the Kaggle historical NSE source."""

    source_name: Literal["kaggle_nse_historical"] = "kaggle_nse_historical"
    source_symbol: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    source_file: str = Field(min_length=1)

    source_timestamp: datetime
    trade_date: date

    open_price: Decimal = Field(gt=0)
    high_price: Decimal = Field(gt=0)
    low_price: Decimal = Field(gt=0)
    close_price: Decimal = Field(gt=0)
    volume_qty: int = Field(ge=0)
    dividends: Decimal = Field(ge=0)
    stock_splits: Decimal = Field(ge=0)

    price_basis: Literal["unknown"] = "unknown"
    ingested_at: datetime
    run_id: str = Field(min_length=1)
    dataset_version: str = Field(min_length=1)

    @field_validator("source_symbol", "symbol")
    @classmethod
    def normalize_symbols(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator("source_timestamp", "ingested_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("Timestamp must include a timezone.")
        return value

    @model_validator(mode="after")
    def validate_record(self) -> "HistoricalSourcePrice":
        expected_trade_date = self.source_timestamp.astimezone(IST).date()

        if self.trade_date != expected_trade_date:
            raise ValueError("trade_date must match source_timestamp in IST.")

        if self.high_price < max(self.open_price, self.close_price, self.low_price):
            raise ValueError("high_price must be at least open, close and low.")

        if self.low_price > min(self.open_price, self.close_price, self.high_price):
            raise ValueError("low_price must be at most open, close and high.")

        return self