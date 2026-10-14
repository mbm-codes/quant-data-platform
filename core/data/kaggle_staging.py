from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from core.data.historical_parquet import (
    write_historical_rejections,
    write_historical_source_prices,
)
from core.data.kaggle_historical import load_kaggle_historical_file_with_rejections


@dataclass(frozen=True)
class HistoricalStagingRunResult:
    files_discovered: int
    files_processed: int
    files_failed: int
    valid_rows: int
    rejected_rows: int
    failed_files: list[str]


def stage_kaggle_historical_directory(
    source_directory: Path,
    staging_directory: Path,
    quarantine_directory: Path,
    *,
    run_id: str,
    dataset_version: str,
    ingested_at: datetime,
) -> HistoricalStagingRunResult:
    """Stage Kaggle historical source files one file at a time."""
    source_paths = sorted(source_directory.glob("*.csv.gz"))
    files_processed = 0
    valid_rows = 0
    rejected_rows = 0
    failed_files: list[str] = []

    for source_path in source_paths:
        try:
            result = load_kaggle_historical_file_with_rejections(
                source_path,
                run_id=run_id,
                dataset_version=dataset_version,
                ingested_at=ingested_at,
            )

            write_historical_source_prices(result.records, staging_directory)
            write_historical_rejections(
                result.rejections,
                quarantine_directory,
                run_id=run_id,
                dataset_version=dataset_version,
                rejected_at=ingested_at,
            )
        except (OSError, ValueError):
            failed_files.append(source_path.name)
            continue

        files_processed += 1
        valid_rows += len(result.records)
        rejected_rows += len(result.rejections)

    return HistoricalStagingRunResult(
        files_discovered=len(source_paths),
        files_processed=files_processed,
        files_failed=len(failed_files),
        valid_rows=valid_rows,
        rejected_rows=rejected_rows,
        failed_files=failed_files,
    )