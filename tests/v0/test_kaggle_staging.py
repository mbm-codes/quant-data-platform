import gzip
from datetime import UTC, datetime
from pathlib import Path

from core.data.kaggle_staging import stage_kaggle_historical_directory

HEADER = "Date,Open,High,Low,Close,Volume,Dividends,Stock Splits\n"

VALID_ROW = (
    "2023-10-31 00:00:00+05:30,"
    "100.00,110.00,90.00,105.00,1000,0.0,0.0\n"
)

INVALID_ROW = (
    "2023-11-01 00:00:00+05:30,"
    "100.00,99.00,90.00,105.00,1000,0.0,0.0\n"
)


def write_gzip_file(path: Path, content: str) -> None:
    path.write_bytes(gzip.compress(content.encode("utf-8")))


def test_stages_valid_rows_and_quarantines_invalid_rows(tmp_path: Path) -> None:
    source_directory = tmp_path / "source"
    source_directory.mkdir()

    write_gzip_file(
        source_directory / "RELIANCE.NS.csv.gz",
        HEADER + VALID_ROW + INVALID_ROW,
    )
    write_gzip_file(
        source_directory / "TCS.NS.csv.gz",
        HEADER + VALID_ROW,
    )

    result = stage_kaggle_historical_directory(
        source_directory,
        tmp_path / "staging",
        tmp_path / "quarantine",
        run_id="test-run",
        dataset_version="kaggle-2023-11",
        ingested_at=datetime.now(UTC),
    )

    assert result.files_discovered == 2
    assert result.files_processed == 2
    assert result.files_failed == 0
    assert result.valid_rows == 2
    assert result.rejected_rows == 1
    assert result.failed_files == []

    assert len(list((tmp_path / "staging").rglob("*.parquet"))) == 2
    assert len(list((tmp_path / "quarantine").rglob("*.parquet"))) == 1