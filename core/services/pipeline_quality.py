from datetime import datetime
from decimal import Decimal
from pathlib import Path

from core.data.duckdb import connect, register_pipeline_runs_view
from core.data.pipeline_run_reader import load_latest_pipeline_run
from core.data_quality import assess_pipeline_quality
from core.models.pipeline_quality import PipelineQualityResult


def get_pipeline_quality(
    *,
    pipeline_name: str,
    warning_rejection_rate: Decimal,
    critical_rejection_rate: Decimal,
    database_path: Path,
    pipeline_runs_directory: Path,
    checked_at: datetime,
) -> PipelineQualityResult:
    """Assess quality of the latest stored run for one pipeline."""
    if not any(pipeline_runs_directory.glob("started_year=*/*.parquet")):
        return assess_pipeline_quality(
            pipeline_run=None,
            pipeline_name=pipeline_name,
            warning_rejection_rate=warning_rejection_rate,
            critical_rejection_rate=critical_rejection_rate,
            checked_at=checked_at,
        )

    connection = connect(database_path)

    try:
        register_pipeline_runs_view(
            connection,
            pipeline_runs_directory,
        )
        pipeline_run = load_latest_pipeline_run(
            connection,
            pipeline_name=pipeline_name,
        )
    finally:
        connection.close()

    return assess_pipeline_quality(
        pipeline_run=pipeline_run,
        pipeline_name=pipeline_name,
        warning_rejection_rate=warning_rejection_rate,
        critical_rejection_rate=critical_rejection_rate,
        checked_at=checked_at,
    )