from datetime import date, datetime

from core.models.data_freshness import DataFreshnessResult


def assess_data_freshness(
    *,
    dataset_name: str,
    as_of_date: date,
    latest_trade_date: date | None,
    max_age_days: int,
    checked_at: datetime,
) -> DataFreshnessResult:
    """Assess whether a dataset has sufficiently recent trading data."""
    if latest_trade_date is None:
        return DataFreshnessResult(
            dataset_name=dataset_name,
            as_of_date=as_of_date,
            checked_at=checked_at,
            max_age_days=max_age_days,
            status="MISSING",
        )

    actual_age_days = (as_of_date - latest_trade_date).days
    status = (
        "FRESH"
        if actual_age_days <= max_age_days
        else "STALE"
    )

    return DataFreshnessResult(
        dataset_name=dataset_name,
        as_of_date=as_of_date,
        latest_trade_date=latest_trade_date,
        checked_at=checked_at,
        max_age_days=max_age_days,
        actual_age_days=actual_age_days,
        status=status,
    )