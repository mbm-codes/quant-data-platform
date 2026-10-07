from pathlib import Path
from typer.testing import CliRunner

from core.cli import app
from core.config.settings import get_settings

runner = CliRunner()

def test_load_and_query_sample_prices(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("QDP_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("QDP_DATABASE_PATH", str(tmp_path / "qdp.duckdb"))
    get_settings.cache_clear()

    fixture_path = (
        Path(__file__).parents[1] / "fixtures" / "v0" / "daily_prices.csv"
    )

    load_result = runner.invoke(app, ["load-csv", str(fixture_path)])

    assert load_result.exit_code == 0
    assert "daily_prices.parquet" in load_result.output

    query_result = runner.invoke(app, ["query", "--symbol", "RELIANCE"])

    assert query_result.exit_code == 0
    assert "RELIANCE" in query_result.output
    assert "TCS" not in query_result.output

    get_settings.cache_clear()