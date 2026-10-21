from datetime import datetime
from pathlib import Path

from core.data.nse_bhavcopy_staging import (
    NseBhavcopyStagingRunResult,
    stage_nse_bhavcopy_directory,
)


def stage_bhavcopy(
    source_directory: Path,
    output_directory: Path,
    *,
    run_id: str,
    dataset_version: str,
    ingested_at: datetime,
) -> NseBhavcopyStagingRunResult:

    result = stage_nse_bhavcopy_directory(
        source_directory,
        output_directory,
        run_id=run_id,
        dataset_version=dataset_version,
        ingested_at=ingested_at,
    )

    return result
