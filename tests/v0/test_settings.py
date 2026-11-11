from decimal import Decimal

import pytest
from pydantic import ValidationError

from core.config.settings import Settings, get_settings


def test_default_settings_are_local() -> None:
    settings = Settings()

    assert settings.env == "local"
    assert settings.data_dir.name == "data"
    assert settings.database_path.name == "qdp.duckdb"


def test_uses_default_daily_refresh_operational_policy(
    monkeypatch,
) -> None:
    for variable_name in (
        "QDP_DAILY_REFRESH_MAX_FRESHNESS_AGE_DAYS",
        "QDP_DAILY_REFRESH_WARNING_REJECTION_RATE",
        "QDP_DAILY_REFRESH_CRITICAL_REJECTION_RATE",
        "QDP_DAILY_REFRESH_MAX_CONSECUTIVE_FAILURES",
    ):
        monkeypatch.delenv(variable_name, raising=False)

    settings = Settings()

    assert settings.daily_refresh_max_freshness_age_days == 3
    assert settings.daily_refresh_warning_rejection_rate == Decimal("0.01")
    assert settings.daily_refresh_critical_rejection_rate == Decimal("0.05")
    assert settings.daily_refresh_max_consecutive_failures == 2


def test_overrides_daily_refresh_operational_policy_from_environment(
    monkeypatch,
) -> None:
    monkeypatch.setenv("QDP_DAILY_REFRESH_MAX_FRESHNESS_AGE_DAYS", "5")
    monkeypatch.setenv("QDP_DAILY_REFRESH_WARNING_REJECTION_RATE", "0.02")
    monkeypatch.setenv("QDP_DAILY_REFRESH_CRITICAL_REJECTION_RATE", "0.10")
    monkeypatch.setenv("QDP_DAILY_REFRESH_MAX_CONSECUTIVE_FAILURES", "3")

    get_settings.cache_clear()
    try:
        settings = get_settings()
    finally:
        get_settings.cache_clear()

    assert settings.daily_refresh_max_freshness_age_days == 5
    assert settings.daily_refresh_warning_rejection_rate == Decimal("0.02")
    assert settings.daily_refresh_critical_rejection_rate == Decimal("0.10")
    assert settings.daily_refresh_max_consecutive_failures == 3


def test_rejects_warning_rejection_rate_above_critical_rate() -> None:
    with pytest.raises(
        ValidationError,
        match="warning_rejection_rate cannot exceed",
    ):
        Settings(
            daily_refresh_warning_rejection_rate=Decimal("0.10"),
            daily_refresh_critical_rejection_rate=Decimal("0.05"),
        )
