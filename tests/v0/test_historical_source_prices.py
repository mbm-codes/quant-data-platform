from datetime import UTC, date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError

from core.models.historical_source_prices import HistoricalSourcePrice

IST = ZoneInfo("Asia/Kolkata")

def valid_source_record() -> dict:
    return {
        "source_symbol": "reliance.ns",
        "symbol": "reliance",
        "source_file": "RELIANCE.NS.csv.gz",
        "source_timestamp": datetime(1996, 1, 1, tzinfo=IST),
        "trade_date": date(1996, 1, 1),
        "open_price": Decimal("10.403049590832516"),
        "high_price": Decimal("10.458870729702461"),
        "low_price": Decimal("10.334541346100153"),
        "close_price": Decimal("10.441109657287598"),
        "volume_qty": 48_051_995,
        "dividends": Decimal(0),
        "stock_splits": Decimal(0),
        "ingested_at": datetime.now(UTC),
        "run_id": "test-run",
        "dataset_version": "kaggle-2023-11",

    }

def test_historical_source_price_normalizes_symbols() -> None:
    record = HistoricalSourcePrice.model_validate(valid_source_record())

    assert record.source_symbol == "RELIANCE.NS"
    assert record.symbol == "RELIANCE"
    assert record.price_basis == "unknown"

def test_historical_source_price_rejects_mismatched_trade_date() -> None:
    record = valid_source_record()
    record["trade_date"] = date(1996, 1, 2)

    with pytest.raises(ValidationError, match="trade_date"):
        HistoricalSourcePrice.model_validate(record)
