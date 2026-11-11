from decimal import Decimal
from functools import lru_cache
from pathlib import Path

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings for the local V0 application."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="QDP_",
        extra="ignore",
    )

    env: str = "local"
    data_dir: Path = Path("data")
    database_path: Path = Path("data/qdp.duckdb")
    daily_refresh_max_freshness_age_days: int = Field(
        default=3,
        ge=0,
    )
    daily_refresh_warning_rejection_rate: Decimal = Field(
        default=Decimal("0.01"),
        ge=Decimal(0),
        le=Decimal(1),
    )
    daily_refresh_critical_rejection_rate: Decimal = Field(
        default=Decimal("0.05"),
        ge=Decimal(0),
        le=Decimal(1),
    )
    daily_refresh_max_consecutive_failures: int = Field(
        default=2,
        gt=0,
    )

    @model_validator(mode="after")
    def validate_operational_policy(self) -> "Settings":
        if (
            self.daily_refresh_warning_rejection_rate
            > self.daily_refresh_critical_rejection_rate
        ):
            raise ValueError(
                "daily_refresh_warning_rejection_rate cannot exceed "
                "daily_refresh_critical_rejection_rate."
            )

        return self


@lru_cache
def get_settings() -> Settings:
    """Return one validated settings instance per process."""

    return Settings()
