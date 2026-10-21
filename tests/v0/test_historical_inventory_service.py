# tests/v0/test_historical_service.py
import gzip
import json
from datetime import UTC, datetime
from pathlib import Path

from core.services.historical_inventory import inventory_historical


def test_inventory_historical_writes_report_for_valid_source_file(
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

    output_path = tmp_path / "reports" / "inventory.json"

    inventory = inventory_historical(
        source_directory=source_directory,
        output_path=output_path,
        run_id="test-run",
        dataset_version="kaggle-2023-11",
        ingested_at=datetime(2024, 1, 3, tzinfo=UTC),
    )

    assert inventory.source_name == "kaggle_nse_historical"
    assert len(inventory.files) == 1
    assert inventory.files[0].source_file == "RELIANCE.NS.csv.gz"
    assert inventory.files[0].row_count == 1
    assert inventory.files[0].error is None

    report = json.loads(output_path.read_text(encoding="utf-8"))

    assert report["files_discovered"] == 1
    assert report["files_processed"] == 1
    assert report["files_failed"] == 0
    assert report["total_valid_rows"] == 1
