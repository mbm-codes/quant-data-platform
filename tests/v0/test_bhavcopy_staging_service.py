import gzip
from datetime import UTC, datetime
from pathlib import Path

from core.services.bhavcopy_staging import stage_bhavcopy


def write_bhavcopy_source_file(path: Path) -> None:
    with gzip.open(path, mode="wt", encoding="utf-8", newline="") as file:
        file.write(
            "SYMBOL,SERIES,DATE1,PREV_CLOSE,OPEN_PRICE,HIGH_PRICE,"
            "LOW_PRICE,LAST_PRICE,CLOSE_PRICE,AVG_PRICE,TTL_TRD_QNTY,"
            "TURNOVER_LACS,NO_OF_TRADES,DELIV_QTY,DELIV_PER\n"
            "RELIANCE,EQ,01-Mar-2024,2900.00,2900.00,2930.00,2890.00,"
            "2915.40,2915.40,2910.00,1000,29.10,50,600,60.00\n"
        )


def test_stage_bhavcopy_stages_valid_equity_prices(tmp_path: Path) -> None:
    source_directory = tmp_path / "source"
    source_directory.mkdir()
    write_bhavcopy_source_file(source_directory / "sec_bhavdata_full_01032024.csv.gz")

    output_directory = tmp_path / "daily_prices"

    result = stage_bhavcopy(
        source_directory=source_directory,
        output_directory=output_directory,
        run_id="test-run",
        dataset_version="nse-bhavcopy-v1",
        ingested_at=datetime(2024, 3, 1, tzinfo=UTC),
    )

    assert result.files_discovered == 1
    assert result.files_processed == 1
    assert result.files_failed == 0
    assert result.valid_rows == 1
    assert len(list(output_directory.rglob("*.parquet"))) == 1
