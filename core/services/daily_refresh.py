from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Literal

from core.models.daily_refresh import DailyRefreshResult
from core.models.pipeline_log_event import PipelineLogEvent
from core.observability.pipeline_logging import emit_pipeline_log_event
from core.services.bhavcopy_staging import (
    stage_bhavcopy as stage_bhavcopy_service,
)
from core.services.data_freshness import get_daily_prices_freshness
from core.services.pipeline_quality import get_pipeline_quality
from core.services.pipeline_runs import (
    complete_pipeline_run,
    fail_pipeline_run,
    start_pipeline_run,
)
from core.data.daily_refresh_ledger import write_daily_refresh_ledger

DailyRefreshStatus = Literal[
    "COMPLETED",
    "COMPLETED_WITH_WARNINGS",
    "FAILED",
]


def run_daily_refresh(
    *,
    source_directory: Path,
    daily_prices_directory: Path,
    database_path: Path,
    pipeline_runs_directory: Path,
    daily_refreshes_directory: Path,
    run_id: str,
    dataset_version: str,
    ingested_at: datetime,
    as_of_date: date,
    max_freshness_age_days: int = 3,
    warning_rejection_rate: Decimal = Decimal("0.01"),
    critical_rejection_rate: Decimal = Decimal("0.05"),
) -> DailyRefreshResult:
    """Stage bhavcopy data and assess its freshness and pipeline quality."""
    pipeline_run = start_pipeline_run(
        run_id=run_id,
        pipeline_name="stage_bhavcopy",
        input_dataset_version=dataset_version,
        output_directory=pipeline_runs_directory,
        started_at=ingested_at,
    )
    emit_pipeline_log_event(
        PipelineLogEvent(
            event="pipeline_started",
            level="INFO",
            pipeline_name="daily_refresh",
            run_id=run_id,
            status="RUNNING",
            timestamp=ingested_at,
            files_discovered=0,
            files_processed=0,
            valid_rows=0,
            rejected_rows=0,
            error_message=None,
        )
    )
    try:
        staging_result = stage_bhavcopy_service(
            source_directory,
            daily_prices_directory,
            run_id=run_id,
            dataset_version=dataset_version,
            ingested_at=ingested_at,
        )
    except Exception as error:
        failed_at = datetime.now(UTC)

        fail_pipeline_run(
            pipeline_run,
            output_directory=pipeline_runs_directory,
            finished_at=failed_at,
            error_message=str(error),
            files_discovered=0,
            files_processed=0,
            valid_rows=0,
            rejected_rows=0,
        )

        emit_pipeline_log_event(
            PipelineLogEvent(
                event="pipeline_failed",
                level="ERROR",
                pipeline_name="daily_refresh",
                run_id=run_id,
                status="FAILED",
                timestamp=failed_at,
                files_discovered=0,
                files_processed=0,
                valid_rows=0,
                rejected_rows=0,
                error_message=str(error),
            )
        )

        failed_result = DailyRefreshResult(
            run_id=run_id,
            status="FAILED",
            started_at=ingested_at,
            finished_at=failed_at,
            files_discovered=0,
            files_processed=0,
            files_failed=0,
            duplicate_files_skipped=0,
            valid_rows=0,
            rejected_rows=0,
            freshness_status="MISSING",
            quality_status="MISSING",
            error_message=str(error),
        )

        write_daily_refresh_ledger(
            [failed_result],
            daily_refreshes_directory,
        )
        raise
    checked_at = datetime.now(UTC)
    complete_pipeline_run(
        pipeline_run,
        output_directory=pipeline_runs_directory,
        finished_at=checked_at,
        files_discovered=staging_result.files_discovered,
        files_processed=staging_result.files_processed,
        valid_rows=staging_result.valid_rows,
        rejected_rows=0,
    )

    freshness_result = get_daily_prices_freshness(
        database_path=database_path,
        daily_prices_directory=daily_prices_directory,
        as_of_date=as_of_date,
        max_age_days=max_freshness_age_days,
        input_dataset_version=dataset_version,
        checked_at=checked_at,
    )

    quality_result = get_pipeline_quality(
        database_path=database_path,
        pipeline_runs_directory=pipeline_runs_directory,
        pipeline_name="stage_bhavcopy",
        warning_rejection_rate=warning_rejection_rate,
        critical_rejection_rate=critical_rejection_rate,
        checked_at=checked_at,
    )

    status, error_message = _determine_refresh_outcome(
        freshness_status=freshness_result.status,
        quality_status=quality_result.status,
    )

    if status == "COMPLETED":
        event_name = "pipeline_completed"
        level = "INFO"
    elif status == "COMPLETED_WITH_WARNINGS":
        event_name = "pipeline_completed_with_warnings"
        level = "WARNING"
    else:
        event_name = "pipeline_failed"
        level = "ERROR"

    emit_pipeline_log_event(
        PipelineLogEvent(
            event=event_name,
            level=level,
            pipeline_name="daily_refresh",
            run_id=run_id,
            status=status,
            timestamp=checked_at,
            files_discovered=staging_result.files_discovered,
            files_processed=staging_result.files_processed,
            valid_rows=staging_result.valid_rows,
            rejected_rows=0,
            error_message=error_message,
        )
    )

    result = DailyRefreshResult(
        run_id=run_id,
        status=status,
        started_at=ingested_at,
        finished_at=checked_at,
        files_discovered=staging_result.files_discovered,
        files_processed=staging_result.files_processed,
        files_failed=staging_result.files_failed,
        duplicate_files_skipped=staging_result.duplicate_files_skipped,
        valid_rows=staging_result.valid_rows,
        rejected_rows=0,
        freshness_status=freshness_result.status,
        quality_status=quality_result.status,
        error_message=error_message,
    )
    write_daily_refresh_ledger(
        [result],
        daily_refreshes_directory,
    )

    return result


def _determine_refresh_outcome(
    *,
    freshness_status: str,
    quality_status: str,
) -> tuple[DailyRefreshStatus, str | None]:
    if freshness_status in {"STALE", "MISSING"}:
        return (
            "FAILED",
            f"Freshness check failed with status {freshness_status}.",
        )

    if quality_status in {"CRITICAL", "MISSING"}:
        return (
            "FAILED",
            f"Pipeline quality check failed with status {quality_status}.",
        )

    if quality_status == "WARNING":
        return ("COMPLETED_WITH_WARNINGS", None)

    return ("COMPLETED", None)
