import gzip
import json
from datetime import UTC, datetime
from pathlib import Path

from core.data.kaggle_inventory import (
    inventory_kaggle_historical_directory,
    write_historical_source_inventory,
)

HEADER = "Date,Open,High,Low,Close,Volume,Dividends,Stock Splits\n"
VALID_ROW = (
    "2023-10-31 00:00:00+05:30,"
    "100.00,110.00,90.00,105.00,1000,0.0,0.0\n"
)

def write_gzip_file(path: Path, content: str) -> None:
    path.write_bytes(gzip.compress(content.encode("utf-8")))


def test_inventories_valid_and_invalid_historical_files(tmp_path: Path) -> None:
    write_gzip_file(tmp_path / "RELIANCE.NS.csv.gz", HEADER + VALID_ROW)
    write_gzip_file(tmp_path / "TCS.NS.csv.gz", HEADER + VALID_ROW)
    write_gzip_file(tmp_path / "BROKEN.NS.csv.gz", "unexpected,column\nvalue,data\n")

    inventory = inventory_kaggle_historical_directory(
        tmp_path,
        run_id="inventory_test",
        dataset_version="kaggle-2023-11",
        ingested_at=datetime.now(UTC),
    )

    report = inventory.to_dict()
    profiles = {file["source_file"]: file for file in report["files"]}
       
    assert report["files_processed"] == 2
    assert report["files_failed"] == 1
    assert report["files_with_rejected_rows"] == 0
    assert report["total_valid_rows"] == 2
    assert report["total_rejected_rows"] == 0
    assert profiles["BROKEN.NS.csv.gz"]["error"] is not None

    output_path = tmp_path / "reports" / "inventory.json"
    write_historical_source_inventory(inventory, output_path)

    saved_report = json.loads(output_path.read_text(encoding="utf-8"))

    assert saved_report["files_processed"] == 2

    