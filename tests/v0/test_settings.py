from core.config.settings import Settings

def test_default_settings_are_local() -> None:
    settings = Settings()

    assert settings.env == "local"
    assert settings.data_dir.name == "data"
    assert settings.database_path.name == "qdp.duckdb"


