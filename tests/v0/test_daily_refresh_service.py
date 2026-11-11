from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from core.services.daily_refresh import run_daily_refresh


def test_completes_when_data_is_fresh_and_quality_is_healthy(
    tmp_path: Path,
    monkeypatch,
) -> None:

    emitted_events = []

    def fake_emit_pipeline_log_event(event):
        emitted_events.append(event)
    captured_arguments: dict[str, dict[str, object]] = {}

    staging_result = SimpleNamespace(
        files_discovered=2,
        files_processed=2,
        files_failed=0,
        duplicate_files_skipped=0,
        valid_rows=3_000,
        rejected_rows=0,
    )
    freshness_result = SimpleNamespace(status="FRESH")
    quality_result = SimpleNamespace(status="HEALTHY")

    written_results = []
    captured_ledger_arguments: dict[str, object] = {}

    def fake_write_daily_refresh_ledger(results, output_directory):
        written_results.extend(results)
        captured_ledger_arguments["output_directory"] = output_directory
        return [output_directory / "refresh_year=2026" / "example.parquet"]

    def fake_stage_bhavcopy_service(*_args, **kwargs):
        captured_arguments["stage"] = kwargs
        return staging_result

    def fake_get_daily_prices_freshness(**kwargs):
        captured_arguments["freshness"] = kwargs
        return freshness_result

    def fake_get_pipeline_quality(**kwargs):
        captured_arguments["quality"] = kwargs
        return quality_result

    pipeline_run = SimpleNamespace(status="RUNNING")

    def fake_start_pipeline_run(**kwargs):
        captured_arguments["start"] = kwargs
        return pipeline_run

    def fake_complete_pipeline_run(*_args, **kwargs):
        captured_arguments["complete"] = kwargs
        return pipeline_run

    monkeypatch.setattr(
        "core.services.daily_refresh.stage_bhavcopy_service",
        fake_stage_bhavcopy_service,
    )
    monkeypatch.setattr(
        "core.services.daily_refresh.get_daily_prices_freshness",
        fake_get_daily_prices_freshness,
    )
    monkeypatch.setattr(
        "core.services.daily_refresh.get_pipeline_quality",
        fake_get_pipeline_quality,
    )
    monkeypatch.setattr(
        "core.services.daily_refresh.start_pipeline_run",
        fake_start_pipeline_run,
    )
    monkeypatch.setattr(
        "core.services.daily_refresh.complete_pipeline_run",
        fake_complete_pipeline_run,
    )
    monkeypatch.setattr(
        "core.services.daily_refresh.emit_pipeline_log_event",
        fake_emit_pipeline_log_event,
    )
    monkeypatch.setattr(
        "core.services.daily_refresh.write_daily_refresh_ledger",
        fake_write_daily_refresh_ledger,
    )

    started_at = datetime(2026, 1, 20, 9, tzinfo=UTC)

    result = run_daily_refresh(
        source_directory=tmp_path / "source",
        daily_prices_directory=tmp_path / "data" / "daily_prices",
        database_path=tmp_path / "qdp.duckdb",
        pipeline_runs_directory=tmp_path / "data" / "pipeline_runs",
        daily_refreshes_directory=tmp_path / "data" / "daily_refreshes",
        run_id="daily-refresh-001",
        dataset_version="nse-bhavcopy-v1",
        ingested_at=started_at,
        as_of_date=date(2026, 1, 20),
        max_freshness_age_days=3,
        warning_rejection_rate=Decimal("0.01"),
        critical_rejection_rate=Decimal("0.05"),
    )

    assert result.status == "COMPLETED"
    assert result.files_discovered == 2
    assert result.files_processed == 2
    assert result.valid_rows == 3_000
    assert result.freshness_status == "FRESH"
    assert result.quality_status == "HEALTHY"

    assert captured_arguments["stage"]["run_id"] == "daily-refresh-001"
    assert captured_arguments["freshness"]["as_of_date"] == date(2026, 1, 20)
    assert captured_arguments["quality"]["pipeline_name"] == "stage_bhavcopy"
    assert captured_arguments["start"]["run_id"] == "daily-refresh-001"
    assert captured_arguments["start"]["pipeline_name"] == "stage_bhavcopy"
    assert captured_arguments["complete"]["files_discovered"] == 2
    assert captured_arguments["complete"]["files_processed"] == 2
    assert captured_arguments["complete"]["valid_rows"] == 3_000
    assert captured_arguments["complete"]["rejected_rows"] == 0
    assert [(event.event, event.status) for event in emitted_events] == [
        ("pipeline_started", "RUNNING"),
        ("pipeline_completed", "COMPLETED"),
    ]

    assert written_results == [result]
    assert captured_ledger_arguments["output_directory"] == (
        tmp_path / "data" / "daily_refreshes"
    )

def test_completes_with_warnings_when_quality_is_warning(
    tmp_path: Path,
    monkeypatch,
) -> None:
    staging_result = SimpleNamespace(
        files_discovered=2,
        files_processed=2,
        files_failed=0,
        duplicate_files_skipped=0,
        valid_rows=3_000,
    )
    freshness_result = SimpleNamespace(status="FRESH")
    quality_result = SimpleNamespace(status="WARNING")

    emitted_events = []

    def fake_emit_pipeline_log_event(event):
        emitted_events.append(event)

    def fake_stage_bhavcopy_service(*_args, **_kwargs):
        return staging_result

    def fake_get_daily_prices_freshness(**_kwargs):
        return freshness_result

    def fake_get_pipeline_quality(**_kwargs):
        return quality_result

    monkeypatch.setattr(
        "core.services.daily_refresh.emit_pipeline_log_event",
        fake_emit_pipeline_log_event,
    )
    monkeypatch.setattr(
        "core.services.daily_refresh.stage_bhavcopy_service",
        fake_stage_bhavcopy_service,
    )
    monkeypatch.setattr(
        "core.services.daily_refresh.get_daily_prices_freshness",
        fake_get_daily_prices_freshness,
    )
    monkeypatch.setattr(
        "core.services.daily_refresh.get_pipeline_quality",
        fake_get_pipeline_quality,
    )

    result = run_daily_refresh(
        source_directory=tmp_path / "source",
        daily_prices_directory=tmp_path / "data" / "daily_prices",
        database_path=tmp_path / "qdp.duckdb",
        pipeline_runs_directory=tmp_path / "data" / "pipeline_runs",
        daily_refreshes_directory=tmp_path / "data" / "daily_refreshes",
        run_id="daily-refresh-002",
        dataset_version="nse-bhavcopy-v1",
        ingested_at=datetime(2026, 1, 20, 9, tzinfo=UTC),
        as_of_date=date(2026, 1, 20),
    )

    assert result.status == "COMPLETED_WITH_WARNINGS"
    assert result.freshness_status == "FRESH"
    assert result.quality_status == "WARNING"
    assert result.error_message is None
    assert [(event.event, event.status) for event in emitted_events] == [
        ("pipeline_started", "RUNNING"),
        (
            "pipeline_completed_with_warnings",
            "COMPLETED_WITH_WARNINGS",
        ),
    ]


@pytest.mark.parametrize(
    ("freshness_status", "quality_status", "expected_message"),
    [
        (
            "STALE",
            "HEALTHY",
            "Freshness check failed with status STALE.",
        ),
        (
            "FRESH",
            "CRITICAL",
            "Pipeline quality check failed with status CRITICAL.",
        ),
    ],
)
def test_fails_when_refresh_quality_gates_fail(
    tmp_path: Path,
    monkeypatch,
    freshness_status: str,
    quality_status: str,
    expected_message: str,
) -> None:
    staging_result = SimpleNamespace(
        files_discovered=2,
        files_processed=2,
        files_failed=0,
        duplicate_files_skipped=0,
        valid_rows=3_000,
    )
    freshness_result = SimpleNamespace(status=freshness_status)
    quality_result = SimpleNamespace(status=quality_status)

    emitted_events = []

    def fake_emit_pipeline_log_event(event):
        emitted_events.append(event)
    def fake_stage_bhavcopy_service(*_args, **_kwargs):
        return staging_result

    def fake_get_daily_prices_freshness(**_kwargs):
        return freshness_result

    def fake_get_pipeline_quality(**_kwargs):
        return quality_result

    monkeypatch.setattr(
        "core.services.daily_refresh.emit_pipeline_log_event",
        fake_emit_pipeline_log_event,
    )
    monkeypatch.setattr(
        "core.services.daily_refresh.stage_bhavcopy_service",
        fake_stage_bhavcopy_service,
    )
    monkeypatch.setattr(
        "core.services.daily_refresh.get_daily_prices_freshness",
        fake_get_daily_prices_freshness,
    )
    monkeypatch.setattr(
        "core.services.daily_refresh.get_pipeline_quality",
        fake_get_pipeline_quality,
    )

    result = run_daily_refresh(
        source_directory=tmp_path / "source",
        daily_prices_directory=tmp_path / "data" / "daily_prices",
        database_path=tmp_path / "qdp.duckdb",
        pipeline_runs_directory=tmp_path / "data" / "pipeline_runs",
                daily_refreshes_directory=tmp_path / "data" / "daily_refreshes",
        run_id="daily-refresh-003",
        dataset_version="nse-bhavcopy-v1",
        ingested_at=datetime(2026, 1, 20, 9, tzinfo=UTC),
        as_of_date=date(2026, 1, 20),
    )

    assert result.status == "FAILED"
    assert result.error_message == expected_message
    assert [(event.event, event.status) for event in emitted_events] == [
        ("pipeline_started", "RUNNING"),
        ("pipeline_failed", "FAILED"),
    ]


def test_records_failed_pipeline_run_when_staging_raises(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured_arguments: dict[str, dict[str, object]] = {}
    pipeline_run = SimpleNamespace(status="RUNNING")

    emitted_events = []

    def fake_emit_pipeline_log_event(event):
        emitted_events.append(event)

    def fake_start_pipeline_run(**kwargs):
        captured_arguments["start"] = kwargs
        return pipeline_run

    def fake_stage_bhavcopy_service(*_args, **_kwargs):
        raise OSError("Unable to read bhavcopy source.")

    def fake_fail_pipeline_run(*_args, **kwargs):
        captured_arguments["failed"] = kwargs
        return pipeline_run

    written_results = []

    def fake_write_daily_refresh_ledger(results, _output_directory):
        written_results.extend(results)
        return []

    monkeypatch.setattr(
        "core.services.daily_refresh.emit_pipeline_log_event",
        fake_emit_pipeline_log_event,
    )
    monkeypatch.setattr(
        "core.services.daily_refresh.start_pipeline_run",
        fake_start_pipeline_run,
    )
    monkeypatch.setattr(
        "core.services.daily_refresh.stage_bhavcopy_service",
        fake_stage_bhavcopy_service,
    )
    monkeypatch.setattr(
        "core.services.daily_refresh.fail_pipeline_run",
        fake_fail_pipeline_run,
    )
    monkeypatch.setattr(
        "core.services.daily_refresh.write_daily_refresh_ledger",
        fake_write_daily_refresh_ledger,
    )

    with pytest.raises(OSError, match="Unable to read bhavcopy source"):
        run_daily_refresh(
            source_directory=tmp_path / "source",
            daily_prices_directory=tmp_path / "data" / "daily_prices",
            database_path=tmp_path / "qdp.duckdb",
            pipeline_runs_directory=tmp_path / "data" / "pipeline_runs",
            daily_refreshes_directory=tmp_path / "data" / "daily_refreshes",
            run_id="daily-refresh-failed-001",
            dataset_version="nse-bhavcopy-v1",
            ingested_at=datetime(2026, 1, 20, 9, tzinfo=UTC),
            as_of_date=date(2026, 1, 20),
        )

    assert len(written_results) == 1
    assert written_results[0].status == "FAILED"
    assert written_results[0].freshness_status == "MISSING"
    assert written_results[0].quality_status == "MISSING"

    assert captured_arguments["start"]["run_id"] == "daily-refresh-failed-001"
    assert captured_arguments["failed"]["error_message"] == (
        "Unable to read bhavcopy source."
    )
    assert captured_arguments["failed"]["files_discovered"] == 0
    assert [(event.event, event.status) for event in emitted_events] == [
        ("pipeline_started", "RUNNING"),
        ("pipeline_failed", "FAILED"),
    ]