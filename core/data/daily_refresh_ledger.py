import hashlib
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from core.models.daily_refresh import DailyRefreshResult

DAILY_REFRESH_LEDGER_SCHEMA = pa.schema(
    [
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("status", pa.string(), nullable=False),
        pa.field("started_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("finished_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("files_discovered", pa.int64(), nullable=False),
        pa.field("files_processed", pa.int64(), nullable=False),
        pa.field("files_failed", pa.int64(), nullable=False),
        pa.field("duplicate_files_skipped", pa.int64(), nullable=False),
        pa.field("valid_rows", pa.int64(), nullable=False),
        pa.field("rejected_rows", pa.int64(), nullable=False),
        pa.field("freshness_status", pa.string(), nullable=False),
        pa.field("quality_status", pa.string(), nullable=False),
        pa.field("error_message", pa.string(), nullable=True),
    ]
)


def write_daily_refresh_ledger(
    results: list[DailyRefreshResult],
    output_directory: Path,
) -> list[Path]:
    """Write immutable daily-refresh outcomes to a Parquet ledger."""
    run_ids = [result.run_id for result in results]

    if len(run_ids) != len(set(run_ids)):
        raise ValueError("Duplicate daily-refresh run ID in write request.")

    output_paths = [
        output_directory
        / f"refresh_year={result.finished_at.year}"
        / _daily_refresh_filename(result.run_id)
        for result in results
    ]

    for result, output_path in zip(results, output_paths, strict=True):
        if output_path.exists():
            raise ValueError(
                f"Daily refresh result already exists in ledger:{result.run_id}"
            )

    written_paths: list[Path] = []

    for result, output_path in zip(results, output_paths, strict=True):
        output_path.parent.mkdir(parents=True, exist_ok=True)

        temporary_path = output_path.with_suffix(".tmp")
        table = pa.Table.from_pylist(
            [result.model_dump(mode="python")],
            schema=DAILY_REFRESH_LEDGER_SCHEMA,
        )

        pq.write_table(table, temporary_path)
        temporary_path.replace(output_path)
        written_paths.append(output_path)

    return written_paths


def _daily_refresh_filename(run_id: str) -> str:
    run_id_hash = hashlib.sha256(run_id.encode("utf-8")).hexdigest()
    return f"run_id={run_id_hash}.parquet"
