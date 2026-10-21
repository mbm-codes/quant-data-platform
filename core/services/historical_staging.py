# core/services/historical_staging.py

from datetime import datetime
from pathlib import Path

from core.data.kaggle_staging import (
    HistoricalStagingRunResult,
    stage_kaggle_historical_directory,
)


def stage_historical(
    source_directory: Path,
    staging_directory: Path,
    quarantine_directory: Path,
    *,
    run_id: str,
    dataset_version: str,
    ingested_at: datetime,
) -> HistoricalStagingRunResult:

    result = stage_kaggle_historical_directory(
        source_directory=source_directory,
        staging_directory=staging_directory,
        quarantine_directory=quarantine_directory,
        run_id=run_id,
        dataset_version=dataset_version,
        ingested_at=ingested_at,
    )
    return result
