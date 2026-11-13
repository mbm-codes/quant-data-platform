from datetime import UTC, date, datetime

import pytest
from pydantic import ValidationError

from core.models.daily_price_coverage import DailyPriceCoverage


def valid_coverage() -> dict[str, object]:
    return {
        "exchange": "NSE",
        "dataset_version": "nse-bhavcopy-v1",
        "total_rows": 1_000,
        "distinct_symbols": 25,
        "first_trade_date": date(2024, 1, 1),
        "last_trade_date": date(2024, 12, 31),
        "duplicate_business_key_count": 0,
        "generated_at": datetime(2026, 1, 1, 9, tzinfo=UTC),
    }


def test_accepts_non_empty_daily_price_coverage() -> None:
    coverage = DailyPriceCoverage.model_validate(valid_coverage())

    assert coverage.dataset_version == "nse-bhavcopy-v1"
    assert coverage.distinct_symbols == 25
    assert coverage.duplicate_business_key_count == 0


def test_accepts_empty_daily_price_coverage() -> None:
    coverage = DailyPriceCoverage(
        dataset_version="nse-bhavcopy-v1",
        total_rows=0,
        distinct_symbols=0,
        duplicate_business_key_count=0,
        generated_at=datetime(2026, 1, 1, 9, tzinfo=UTC),
    )

    assert coverage.first_trade_date is None
    assert coverage.last_trade_date is None


def test_rejects_dates_in_reverse_order() -> None:
    record = valid_coverage()
    record["first_trade_date"] = date(2025, 1, 1)

    with pytest.raises(ValidationError, match="first_trade_date cannot be after"):
        DailyPriceCoverage.model_validate(record)


def test_rejects_non_empty_coverage_without_trade_dates() -> None:
    record = valid_coverage()
    record["first_trade_date"] = None

    with pytest.raises(ValidationError, match="requires first and last"):
        DailyPriceCoverage.model_validate(record)


def test_rejects_more_symbols_than_rows() -> None:
    record = valid_coverage()
    record["distinct_symbols"] = 1_001

    with pytest.raises(ValidationError, match="distinct_symbols cannot exceed"):
        DailyPriceCoverage.model_validate(record)