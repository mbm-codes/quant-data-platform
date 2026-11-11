from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from core.models.daily_refresh_health import DailyRefreshHealth


def valid_health() -> dict[str, object]:
    return {
        "status": "HEALTHY",
        "latest_run_id": "daily-refresh-001",
        "latest_refresh_status": "COMPLETED",
        "latest_finished_at": datetime(2026, 1, 20, 9, 2, tzinfo=UTC),
        "consecutive_failure_count": 0,
        "max_consecutive_failures": 2,
        "reason": "Latest refresh completed successfully.",
        "checked_at": datetime(2026, 1, 20, 9, 3, tzinfo=UTC),
    }


def test_accepts_healthy_daily_refresh_state() -> None:
    health = DailyRefreshHealth.model_validate(valid_health())

    assert health.status == "HEALTHY"
    assert health.consecutive_failure_count == 0


def test_accepts_missing_daily_refresh_state() -> None:
    health = DailyRefreshHealth(
        status="MISSING",
        consecutive_failure_count=0,
        max_consecutive_failures=2,
        reason="No refresh results exist yet.",
        checked_at=datetime(2026, 1, 20, 9, tzinfo=UTC),
    )

    assert health.status == "MISSING"


def test_rejects_healthy_state_with_failed_latest_refresh() -> None:
    record = valid_health()
    record["latest_refresh_status"] = "FAILED"
    record["consecutive_failure_count"] = 1

    with pytest.raises(ValidationError, match="HEALTHY health requires"):
        DailyRefreshHealth.model_validate(record)


def test_rejects_noncritical_critical_state() -> None:
    record = valid_health()
    record["status"] = "CRITICAL"

    with pytest.raises(ValidationError, match="CRITICAL health requires"):
        DailyRefreshHealth.model_validate(record)


def test_rejects_missing_state_with_latest_run_fields() -> None:
    record = valid_health()
    record["status"] = "MISSING"

    with pytest.raises(ValidationError, match="MISSING health cannot include"):
        DailyRefreshHealth.model_validate(record)


def test_accepts_warning_for_first_consecutive_failure() -> None:
    record = valid_health()
    record["status"] = "WARNING"
    record["latest_refresh_status"] = "FAILED"
    record["consecutive_failure_count"] = 1
    record["reason"] = "One consecutive refresh failure."

    health = DailyRefreshHealth.model_validate(record)

    assert health.status == "WARNING"


def test_accepts_critical_state_at_consecutive_failure_limit() -> None:
    record = valid_health()
    record["status"] = "CRITICAL"
    record["latest_refresh_status"] = "FAILED"
    record["consecutive_failure_count"] = 2
    record["reason"] = "Consecutive failure limit reached."

    health = DailyRefreshHealth.model_validate(record)

    assert health.status == "CRITICAL"