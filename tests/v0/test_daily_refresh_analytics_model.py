from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from core.models.daily_refresh_analytics import DailyRefreshSummary


def valid_summary() -> dict[str, object]:
    return {
        "total_refreshes": 4,
        "completed_count": 2,
        "completed_with_warnings_count": 1,
        "failed_count": 1,
        "success_rate": Decimal("0.75"),
        "latest_successful_finished_at": datetime(
            2026,
            1,
            20,
            9,
            2,
            tzinfo=UTC,
        ),
        "generated_at": datetime(2026, 1, 21, 9, tzinfo=UTC),
    }


def test_accepts_valid_daily_refresh_summary() -> None:
    summary = DailyRefreshSummary.model_validate(valid_summary())

    assert summary.total_refreshes == 4
    assert summary.success_rate == Decimal("0.75")


def test_rejects_counts_that_do_not_add_up() -> None:
    record = valid_summary()
    record["failed_count"] = 2

    with pytest.raises(ValidationError, match="add up"):
        DailyRefreshSummary.model_validate(record)


def test_rejects_incorrect_success_rate() -> None:
    record = valid_summary()
    record["success_rate"] = Decimal("0.50")

    with pytest.raises(ValidationError, match="success_rate"):
        DailyRefreshSummary.model_validate(record)


def test_rejects_missing_latest_time_when_refreshes_succeeded() -> None:
    record = valid_summary()
    record["latest_successful_finished_at"] = None

    with pytest.raises(ValidationError, match="require a latest successful time"):
        DailyRefreshSummary.model_validate(record)