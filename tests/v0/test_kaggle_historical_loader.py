import gzip
from datetime import datetime, timezone
from pathlib import Path
from decimal import Decimal

from core.data.kaggle_historical import load_kaggle_historical_file

def write_historical_fixture(path: Path) -> None:
    path.write_bytes(
        gzip.compress(
            (
                "Date,Open,High,Low,Close,Volume,Dividends,Stock Splits\n"
                "1997-03-28 00:00:00+05:30,"
                "15.412372589111326,15.412372589111326,"
                "15.412372589111326,15.412372589111328,"
                "0,0.0,0.0\n"
            ).encode("utf-8")
        )
    )

def test_loads_kaggle_historical_gzip_file(tmp_path: Path) -> None:
    source_path = tmp_path / "RELIANCE.NS.csv.gz"
    write_historical_fixture(source_path)

    records = load_kaggle_historical_file(
        source_path,
        run_id="test_run",
        dataset_version="kaggle-2023-11",
        ingested_at=datetime.now(timezone.utc),
    )

    assert len(records) == 1
    assert records[0].source_symbol == "RELIANCE.NS"
    assert records[0].symbol == "RELIANCE"
    assert records[0].trade_date.isoformat() == "1997-03-28"
    assert records[0].price_basis == "unknown"
    assert records[0].high_price == Decimal("15.41237259")
    assert records[0].close_price == Decimal("15.41237259")


