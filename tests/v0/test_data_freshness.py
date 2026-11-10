from datetime import UTC, date, datetime

from core.data_freshness import assess_data_freshness


def test_marks_recent_dataset_as_fresh() -> None:
    result = assess_data_freshness(
        dataset_name="daily_prices",
        as_of_date=date(2026, 1, 20),
        latest_trade_date=date(2026, 1, 20),
        max_age_days=3,
        checked_at=datetime(2026, 1, 20, 18, tzinfo=UTC),
    )

    assert result.status == "FRESH"
    assert result.actual_age_days == 0


def test_marks_old_dataset_as_stale() -> None:
    result = assess_data_freshness(
        dataset_name="daily_prices",
        as_of_date=date(2026, 1, 20),
        latest_trade_date=date(2026, 1, 15),
        max_age_days=3,
        checked_at=datetime(2026, 1, 20, 18, tzinfo=UTC),
    )

    assert result.status == "STALE"
    assert result.actual_age_days == 5


def test_marks_absent_dataset_as_missing() -> None:
    result = assess_data_freshness(
        dataset_name="daily_prices",
        as_of_date=date(2026, 1, 20),
        latest_trade_date=None,
        max_age_days=3,
        checked_at=datetime(2026, 1, 20, 18, tzinfo=UTC),
    )

    assert result.status == "MISSING"
    assert result.latest_trade_date is None
    assert result.actual_age_days is None