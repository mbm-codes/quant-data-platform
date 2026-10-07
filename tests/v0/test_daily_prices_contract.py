from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from pydantic import ValidationError

from core.data.daily_prices import load_daily_prices_csv
from core.data.duckdb import connect, register_daily_prices_view
from core.data.parquet import write_daily_prices_parquet
from core.models.daily_prices import DailyPrice


def valid_record() -> dict:
    return {
        "exchange": "NSE",
        "symbol": "reliance",
        "trade_date": date(2026, 9, 24),
        "open_price": Decimal("1400.00"),
        "high_price": Decimal("1425.00"),
        "low_price": Decimal("1390.00"),
        "close_price": Decimal("1415.00"),
        "volume_qty": 1_000_000,
        "source_name": "sample",
        "source_file": "prices.csv",
        "ingested_at": datetime.now(UTC),
        "run_id": "test-run",
        "dataset_version": "v0",
    }

def test_valid_daily_price_normalizes_symbol() -> None:
    record = DailyPrice.model_validate(valid_record())

    assert record.symbol == "RELIANCE"
    assert record.price_basis == "raw"


def test_daily_price_rejects_invalid_ohlc_values() -> None:
    record = valid_record()
    record["high_price"] = Decimal("1300.00")

    with pytest.raises(ValidationError, match="high_price"):
        DailyPrice.model_validate(record)


def test_loads_and_validates_sample_csv() -> None:
    fixture_path = (
        Path(__file__).parents[1] / "fixtures" / "v0" / "daily_prices.csv"
    )

    records = load_daily_prices_csv(fixture_path)

    assert len(records) == 4
    assert records[0].symbol == "RELIANCE"
    assert records[0].close_price == Decimal("1415.00")

def test_writes_sample_data_to_partitioned_parquet(tmp_path: Path) -> None:
    fixture_path = (
        Path(__file__).parents[1] / "fixtures" / "v0" / "daily_prices.csv"
    )

    records = load_daily_prices_csv(fixture_path)
    written_paths = write_daily_prices_parquet(records, tmp_path)

    assert written_paths == [tmp_path / "trade_year=2026" / "daily_prices.parquet"]

    table = pq.read_table(written_paths[0])

    assert table.num_rows == 4
    assert table.column("symbol").to_pylist() == [
        "RELIANCE",
        "RELIANCE",
        "TCS",
        "TCS"
    ]

def test_queries_parquet_data_with_duckdb(tmp_path: Path) -> None:
    fixture_path = (
        Path(__file__).parents[1] / "fixtures" / "v0" / "daily_prices.csv"
    )

    records = load_daily_prices_csv(fixture_path)

    parquet_directory = tmp_path / "daily_prices"
    write_daily_prices_parquet(records, parquet_directory)

    connection = connect(tmp_path / "qdp.duckdb")
    register_daily_prices_view(connection, parquet_directory)

    rows = connection.execute(
        """
        SELECT symbol, close_price, trade_year
        FROM daily_prices
        ORDER BY symbol, trade_date
        """
    ).fetchall()

    connection.close()

    assert rows == [
        ("RELIANCE", Decimal("1415.00000000"), 2026),
        ("RELIANCE", Decimal("1422.00000000"), 2026),
        ("TCS", Decimal("3235.00000000"), 2026),
        ("TCS", Decimal("3250.00000000"), 2026),
    ]