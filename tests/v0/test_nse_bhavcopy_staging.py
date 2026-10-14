import gzip
from datetime import UTC, datetime
from pathlib import Path

from core.data.nse_bhavcopy_staging import stage_nse_bhavcopy_directory

HEADER = (
    "SYMBOL, SERIES, DATE1, PREV_CLOSE, OPEN_PRICE, HIGH_PRICE, "
    "LOW_PRICE, LAST_PRICE, CLOSE_PRICE, AVG_PRICE, TTL_TRD_QNTY, "
    "TURNOVER_LACS, NO_OF_TRADES, DELIV_QTY, DELIV_PER\n"
)


def write_bhavcopy_fixture(path: Path, row: str) -> None:
    with gzip.open(path, mode="wt", encoding="utf-8", newline="") as source_file:
        source_file.write(HEADER + row)


def test_stages_each_bhavcopy_file_to_canonical_parquet(tmp_path: Path) -> None:
    source_directory = tmp_path / "source"
    source_directory.mkdir()

    write_bhavcopy_fixture(
        source_directory / "sec_bhavdata_full_01012024.csv.gz",
        "RELIANCE, EQ, 01-Jan-2024, 2500, 2510, 2530, 2505, "
        "2520, 2520, 2518, 1000, 25.18, 50, 600, 60.00\n",
    )

    write_bhavcopy_fixture(
        source_directory / "sec_bhavdata_full_02012024.csv.gz",
        "TCS, EQ, 02-Jan-2024, 3500, 3510, 3530, 3505, "
        "3520, 3520, 3518, 2000, 70.36, 60, 1200, 60.00\n",
    )

    output_directory = tmp_path / "daily_prices"
    result = stage_nse_bhavcopy_directory(
        source_directory,
        output_directory,
        run_id="test-run",
        dataset_version="nse-bhavcopy-v1",
        ingested_at=datetime.now(UTC),
    )

    assert result.files_discovered == 2
    assert result.files_processed == 2
    assert result.files_failed == 0
    assert result.valid_rows == 2
    assert result.failed_files == []
    assert len(list(output_directory.rglob("*.parquet"))) == 2


def test_skips_identical_duplicate_trade_dates(tmp_path: Path) -> None:
    source_directory = tmp_path / "source"
    source_directory.mkdir()

    duplicate_row = (
        "RELIANCE, EQ, 03-Nov-2023, 2500, 2510, 2530, 2505, "
        "2520, 2520, 2518, 1000, 25.18, 50, 600, 60.00\n"
    )

    write_bhavcopy_fixture(
        source_directory / "sec_bhavdata_full_03112023.csv.gz",
        duplicate_row,
    )

    write_bhavcopy_fixture(
        source_directory / "sec_bhavdata_full_05112023.csv.gz",
        duplicate_row,
    )

    output_directory = tmp_path / "daily_prices"
    result = stage_nse_bhavcopy_directory(
        source_directory,
        output_directory,
        run_id="test-run",
        dataset_version="nse-bhavcopy-v1",
        ingested_at=datetime.now(UTC),
    )

    assert result.files_discovered == 2
    assert result.files_processed == 2
    assert result.files_failed == 0
    assert result.duplicate_files_skipped == 1
    assert result.valid_rows == 1
    assert len(list(output_directory.rglob("*.parquet"))) == 1
