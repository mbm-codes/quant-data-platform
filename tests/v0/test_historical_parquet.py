import gzip
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

from core.data.historical_parquet import (
    write_historical_rejections,
    write_historical_source_prices,
)
from core.data.kaggle_historical import (
    load_kaggle_historical_file_with_rejections,
)

HEADER = "Date,Open,High,Low,Close,Volume,Dividends,Stock Splits\n"

SOURCE_CONTENT = (
    HEADER
    + "2023-10-31 00:00:00+05:30,"
    "100.00,110.00,90.00,105.00,1000,0.0,0.0\n"
    + "2023-11-01 00:00:00+05:30,"
    "100.00,99.00,90.00,105.00,1000,0.0,0.0\n"
)

def test_writes_staging_and_quarantine_parquet(tmp_path: Path) -> None:
    source_path = tmp_path / "RELIANCE.NS.csv.gz"
    source_path.write_bytes(gzip.compress(SOURCE_CONTENT.encode("utf-8")))

    ingested_at = datetime.now(UTC)
    result = load_kaggle_historical_file_with_rejections(
        source_path,
        run_id="test-run",
        dataset_version="kaggle-2023-11",
        ingested_at=ingested_at,
    )

    staging_paths = write_historical_source_prices(
        result.records,
        tmp_path / "staging",
    )

    quarantine_paths = write_historical_rejections(
        result.rejections,
        tmp_path / "quarantine",
        run_id="test-run",
        dataset_version="kaggle-2023-11",
        rejected_at=ingested_at,
    )

    assert len(staging_paths) == 1
    assert len(quarantine_paths) == 1
    assert pq.read_table(staging_paths[0]).num_rows == 1
    assert pq.read_table(quarantine_paths[0]).num_rows == 1

    quarantine_row = pq.read_table(quarantine_paths[0]).to_pylist()[0]

    assert quarantine_row["source_file"] == "RELIANCE.NS.csv.gz"
    assert quarantine_row["line_number"] == 3
    assert "high_price must be at least" in quarantine_row["reason"]

    repeated_staging_paths = write_historical_source_prices(
        result.records,
        tmp_path / "staging",
    )

    assert repeated_staging_paths == staging_paths
    assert pq.read_table(staging_paths[0]).num_rows == 1

    