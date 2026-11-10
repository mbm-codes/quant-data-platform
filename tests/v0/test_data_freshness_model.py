from datetime import UTC, date, datetime

import pytest
from pydantic import ValidationError

from core.models.data_freshness import DataFreshnessResult


def valid_freshness_result() -> dict[str, object]:
    return {
        "dataset_name": "daily_prices",
        "as_of_date": date(2026, 1, 20),
        "latest_trade_date": date(2026, 1, 20),
        "checked_at": datetime(2026, 1, 20, 18, tzinfo=UTC),
        "max_age_days": 1,
        "actual_age_days": 0,
        "status": "FRESH",
    }


def test_accepts_fresh_result() -> None:
    result = DataFreshnessResult.model_validate(valid_freshness_result())

    assert result.status == "FRESH"
    assert result.actual_age_days == 0


def test_accepts_stale_result() -> None:
    record = valid_freshness_result()
    record["latest_trade_date"] = date(2026, 1, 15)
    record["actual_age_days"] = 5
    record["status"] = "STALE"

    result = DataFreshnessResult.model_validate(record)

    assert result.status == "STALE"


def test_accepts_missing_result() -> None:
    record = valid_freshness_result()
    record["latest_trade_date"] = None
    record["actual_age_days"] = None
    record["status"] = "MISSING"

    result = DataFreshnessResult.model_validate(record)

    assert result.status == "MISSING"


def test_rejects_fresh_result_above_max_age() -> None:
    record = valid_freshness_result()
    record["actual_age_days"] = 2

    with pytest.raises(
        ValidationError,
        match="FRESH results cannot exceed max_age_days",
    ):
        DataFreshnessResult.model_validate(record)


def test_rejects_stale_result_within_max_age() -> None:
    record = valid_freshness_result()
    record["actual_age_days"] = 1
    record["status"] = "STALE"

    with pytest.raises(
        ValidationError,
        match="STALE results must exceed max_age_days",
    ):
        DataFreshnessResult.model_validate(record)


def test_rejects_naive_checked_at() -> None:
    record = valid_freshness_result()
    record["checked_at"] = datetime(2026, 1, 20, 18)  # noqa: DTZ001

    with pytest.raises(
        ValidationError,
        match="checked_at must include timezone information",
    ):
        DataFreshnessResult.model_validate(record)