from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from core.data.pipeline_run_store import write_pipeline_run
from core.models.pipeline_run import PipelineRun
from core.services.pipeline_quality import get_pipeline_quality


def make_completed_run() -> PipelineRun:
    return PipelineRun(
        run_id="stage-bhavcopy-001",
        pipeline_name="stage_bhavcopy",
        input_dataset_version="nse-bhavcopy-v1",
        status="COMPLETED",
        started_at=datetime(2026, 1, 20, 9, tzinfo=UTC),
        finished_at=datetime(2026, 1, 20, 9, 2, tzinfo=UTC),
        files_discovered=1,
        files_processed=1,
        valid_rows=1_818,
        rejected_rows=0,
    )


def test_reports_quality_for_latest_pipeline_run(tmp_path: Path) -> None:
    pipeline_runs_directory = tmp_path / "pipeline_runs"
    write_pipeline_run(make_completed_run(), pipeline_runs_directory)

    result = get_pipeline_quality(
        pipeline_name="stage_bhavcopy",
        warning_rejection_rate=Decimal("0.01"),
        critical_rejection_rate=Decimal("0.05"),
        database_path=tmp_path / "qdp.duckdb",
        pipeline_runs_directory=pipeline_runs_directory,
        checked_at=datetime(2026, 1, 20, 18, tzinfo=UTC),
    )

    assert result.status == "HEALTHY"
    assert result.latest_run_id == "stage-bhavcopy-001"


def test_reports_missing_when_no_pipeline_run_exists(
    tmp_path: Path,
) -> None:
    result = get_pipeline_quality(
        pipeline_name="stage_bhavcopy",
        warning_rejection_rate=Decimal("0.01"),
        critical_rejection_rate=Decimal("0.05"),
        database_path=tmp_path / "qdp.duckdb",
        pipeline_runs_directory=tmp_path / "pipeline_runs",
        checked_at=datetime(2026, 1, 20, 18, tzinfo=UTC),
    )

    assert result.status == "MISSING"