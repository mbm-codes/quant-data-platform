# tests/v0/test_historical_staging_service.py
import gzip
from datetime import UTC, datetime
from pathlib import Path

from core.services.historical_staging import stage_historical


def test_stage_historical_stages_valid_source_file(
    tmp_path: Path,
) -> None:
    source_directory = tmp_path / "source"
    source_directory.mkdir()
    source_file = source_directory / "RELIANCE.NS.csv.gz"
    with gzip.open(source_file, mode="wt", encoding="utf-8", newline="") as file:
        file.write(
            "Date,Open,High,Low,Close,Volume,Dividends,Stock Splits\n"
            "2024-01-02 00:00:00+05:30,"
            "2500.00,2530.00,2490.00,2515.00,1000,0.0,0.0\n"
        )
    staging_dir = (
        tmp_path / "staging" / "kaggle_historical_prices" / "run_id=test-run-1"
    )
    quarantine_dir = tmp_path / "quarantine" / "kaggle_historical_rejections"

    result = stage_historical(
        source_directory=source_directory,
        staging_directory=staging_dir,
        quarantine_directory=quarantine_dir,
        run_id="test-run-1",
        dataset_version="v1",
        ingested_at=datetime(2025, 1, 4, tzinfo=UTC),
    )

    assert result.files_discovered == 1
    assert result.files_processed == 1
    assert result.files_failed == 0
    assert result.valid_rows == 1


def test_stage_historical_stages_invalid_OHLC_source_file(
    tmp_path: Path,
) -> None:
    source_directory = tmp_path / "source"
    source_directory.mkdir()
    source_file = source_directory / "RELIANCE.NS.csv.gz"
    with gzip.open(source_file, mode="wt", encoding="utf-8", newline="") as file:
        file.write(
            "Date,Open,High,Low,Close,Volume,Dividends,Stock Splits\n"
            "2024-01-02 00:00:00+05:30,"
            "2500.00,2501.00,2490.00,2515.00,1000,0.0,0.0\n"
        )
    staging_dir = (
        tmp_path / "staging" / "kaggle_historical_prices" / "run_id=test-run-1"
    )
    quarantine_dir = tmp_path / "quarantine" / "kaggle_historical_rejections"

    result = stage_historical(
        source_directory=source_directory,
        staging_directory=staging_dir,
        quarantine_directory=quarantine_dir,
        run_id="test-run-1",
        dataset_version="v1",
        ingested_at=datetime(2025, 1, 4, tzinfo=UTC),
    )

    assert result.files_discovered == 1
    assert result.files_processed == 1
    assert result.files_failed == 0
    assert result.failed_files == []
    assert result.valid_rows == 0
    assert result.rejected_rows == 1

    assert len(list(staging_dir.rglob("*.parquet"))) == 0
    assert len(list(quarantine_dir.rglob("*.parquet"))) == 1
