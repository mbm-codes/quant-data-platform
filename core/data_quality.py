from datetime import datetime
from decimal import Decimal

from core.models.pipeline_quality import PipelineQualityResult
from core.models.pipeline_run import PipelineRun


def assess_pipeline_quality(
    *,
    pipeline_run: PipelineRun | None,
    warning_rejection_rate: Decimal,
    critical_rejection_rate: Decimal,
    checked_at: datetime,
    pipeline_name: str | None = None,
) -> PipelineQualityResult:
    """Assess pipeline health from its latest stored run record."""
    if pipeline_run is None:
        if not pipeline_name:
            raise ValueError(
                "pipeline_name is required when pipeline_run is missing."
            )

        return PipelineQualityResult(
            pipeline_name=pipeline_name,
            warning_rejection_rate=warning_rejection_rate,
            critical_rejection_rate=critical_rejection_rate,
            status="MISSING",
            checked_at=checked_at,
        )

    total_rows = pipeline_run.valid_rows + pipeline_run.rejected_rows
    rejection_rate = (
        Decimal(pipeline_run.rejected_rows) / Decimal(total_rows)
        if total_rows > 0
        else Decimal(0)
    )

    if (
        pipeline_run.status == "FAILED"
        or rejection_rate > critical_rejection_rate
    ):
        status = "CRITICAL"
    elif (
        pipeline_run.status in {"RUNNING", "COMPLETED_WITH_WARNINGS"}
        or rejection_rate > warning_rejection_rate
    ):
        status = "WARNING"
    else:
        status = "HEALTHY"

    return PipelineQualityResult(
        pipeline_name=pipeline_run.pipeline_name,
        latest_run_id=pipeline_run.run_id,
        latest_run_status=pipeline_run.status,
        files_discovered=pipeline_run.files_discovered,
        files_processed=pipeline_run.files_processed,
        valid_rows=pipeline_run.valid_rows,
        rejected_rows=pipeline_run.rejected_rows,
        rejection_rate=rejection_rate,
        warning_rejection_rate=warning_rejection_rate,
        critical_rejection_rate=critical_rejection_rate,
        status=status,
        checked_at=checked_at,
    )