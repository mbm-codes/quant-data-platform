import hashlib
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from core.data.kaggle_historical import HistoricalRowRejection
from core.models.historical_source_prices import HistoricalSourcePrice

HISTORICAL_STAGING_SCHEMA = pa.schema(
    [
        pa.field("source_name", pa.string(), nullable=False),
        pa.field("source_symbol", pa.string(), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("source_file", pa.string(), nullable=False),
        pa.field("source_timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("trade_date", pa.date32(), nullable=False),
        pa.field("open_price", pa.decimal128(20, 8), nullable=False),
        pa.field("high_price", pa.decimal128(20, 8), nullable=False),
        pa.field("low_price", pa.decimal128(20, 8), nullable=False),
        pa.field("close_price", pa.decimal128(20, 8), nullable=False),
        pa.field("volume_qty", pa.int64(), nullable=False),
        pa.field("dividends", pa.decimal128(20, 8), nullable=False),
        pa.field("stock_splits", pa.decimal128(20, 8), nullable=False),
        pa.field("price_basis", pa.string(), nullable=False),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("dataset_version", pa.string(), nullable=False),
    ]
)

HISTORICAL_QUARANTINE_SCHEMA = pa.schema(
    [
        pa.field("source_file", pa.string(), nullable=False),
        pa.field("source_symbol", pa.string(), nullable=False),
        pa.field("line_number", pa.int64(), nullable=False),
        pa.field("reason", pa.string(), nullable=False),
        pa.field("raw_row_json", pa.string(), nullable=False),
        pa.field("rejected_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("dataset_version", pa.string(), nullable=False),
    ]
)

def write_historical_source_prices(
        records: list[HistoricalSourcePrice],
        output_directory: Path,
) -> list[Path]:
    """Write one source file's validated rows into year-partitioned Parquet."""

    if not records:
        return []

    source_files = {record.source_file for record in records}
    if len(source_files) != 1:
        raise ValueError("Expected records from exactly one source file.")

    records_by_year: dict[int, list[HistoricalSourcePrice]] = defaultdict(list)

    for record in records:
        records_by_year[record.trade_date.year].append(record)

    source_token = _source_file_token(next(iter(source_files)))
    written_paths: list[Path] = []

    for trade_year, year_records in records_by_year.items():
        output_path = (
            output_directory
            / f"trade_year={trade_year}"
            / f"{source_token}.parquet"
        )

        table = pa.Table.from_pylist(
            [record.model_dump(mode="python") for record in year_records],
            schema=HISTORICAL_STAGING_SCHEMA,
        )

        _write_table_atomically(table, output_path)
        written_paths.append(output_path)

    return written_paths

def write_historical_rejections(
        rejections: list[HistoricalRowRejection],
        output_directory: Path,
        *,
        run_id: str,
        dataset_version: str,
        rejected_at: datetime,
) -> list[Path]:
    """Write one source file's rejected rows to a quarantine Parquet dataset."""
    if not rejections:
        return []

    source_files = {rejection.source_file for rejection in rejections}
    if len(source_files) != 1:
        raise ValueError("Expected rejections from exaclty one source file.")

    source_file = next(iter(source_files))
    output_path = (
        output_directory
        / f"run_id={run_id}"
        / f"{_source_file_token(source_file)}.parquet"
    )

    rows = [
        {
            "source_file": rejection.source_file,
            "source_symbol": rejection.source_symbol,
            "line_number": rejection.line_number,
            "reason": rejection.reason,
            "raw_row_json": json.dumps(rejection.raw_row, sort_keys=True),
            "rejected_at": rejected_at,
            "run_id": run_id,
            "dataset_version": dataset_version,
        }
        for rejection in rejections
    ]

    table = pa.Table.from_pylist(rows, schema=HISTORICAL_QUARANTINE_SCHEMA)
    _write_table_atomically(table, output_path)

    return [output_path]

def _write_table_atomically(table: pa.Table, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_suffix(".tmp")

    pq.write_table(table, temporary_path)
    temporary_path.replace(output_path)

def _source_file_token(source_file: str) -> str:
    return hashlib.sha256(source_file.encode("utf-8")).hexdigest()[:16]