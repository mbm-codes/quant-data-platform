from functools import lru_cache
from pathlib import Path
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

@lru_cache
def get_settings() -> Settings:
    """Return one validated settings instance per process."""

    return Settings()

