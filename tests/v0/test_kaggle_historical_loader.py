import gzip
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from core.data.kaggle_historical import (
    load_kaggle_historical_file,
)

HEADER_WITH_CAPITAL_GAINS = (
    "Date,Open,High,Low,Close,Volume,Dividends,Stock Splits,Capital Gains\n"
)

HEADER = "Date,Open,High,Low,Close,Volume,Dividends,Stock Splits\n"

DEFAULT_HISTORICAL_CONTENT = (
    HEADER
    + "1997-03-28 00:00:00+05:30,"
    "15.412372589111326,15.412372589111326,"
    "15.412372589111326,15.412372589111328,"
    "0,0.0,0.0\n"
)

def write_historical_fixture(
        path: Path, 
        content: str = DEFAULT_HISTORICAL_CONTENT) -> None:
    path.write_bytes(gzip.compress(content.encode("utf-8")))

def test_loads_kaggle_historical_gzip_file(tmp_path: Path) -> None:
    source_path = tmp_path / "RELIANCE.NS.csv.gz"
    write_historical_fixture(source_path)

    records = load_kaggle_historical_file(
        source_path,
        run_id="test_run",
        dataset_version="kaggle-2023-11",
        ingested_at=datetime.now(UTC),
    )

    assert len(records) == 1
    assert records[0].source_symbol == "RELIANCE.NS"
    assert records[0].symbol == "RELIANCE"
    assert records[0].trade_date.isoformat() == "1997-03-28"
    assert records[0].price_basis == "unknown"
    assert records[0].high_price == Decimal("15.41237259")
    assert records[0].close_price == Decimal("15.41237259")
    assert records[0].dividends == Decimal("0.00000000")
    assert records[0].stock_splits == Decimal("0.00000000")


def test_quarantines_invalid_rows_and_retains_valid_rows(
        tmp_path: Path
) -> None:
    source_path = tmp_path / "RELIANCE.NS.csv.gz"
    write_historical_fixture(
        source_path,
        (
            HEADER
            + "2023-10-31 00:00:00+05:30,"
            "100.00,110.00,90.00,105.00,1000,0.0,0.0\n"
            + "2023-11-01 00:00:00+05:30,"
            "100.00,99.00,90.00,105.00,1000,0.0,0.0\n"
        ),
    )