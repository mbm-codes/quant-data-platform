# tests/v0/test_cli.py

import gzip
import pyarrow.parquet as pq

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from core.cli import app
from core.config.settings import get_settings
from core.data.parquet import write_daily_prices_parquet
from core.models.daily_prices import DailyPrice

runner = CliRunner()


def configure_test_settings(monkeypatch, tmp_path: Path) -> Path:
    data_dir = tmp_path / "data"

    monkeypatch.setattr(
        "core.cli.get_settings",
        lambda: SimpleNamespace(
            data_dir=data_dir,
            database_path=tmp_path / "qdp.duckdb",
        ),
    )

    return data_dir


def write_recording_test_prices(output_directory: Path) -> None:
    start_date = date(2024, 1, 1)
    close_values = [Decimal("100")] + [Decimal("101")] * 19 + [Decimal("121")]

    prices = [
        DailyPrice(
            exchange="NSE",
            symbol="RELIANCE",
            trade_date=start_date + timedelta(days=index),
            open_price=close_price,
            high_price=close_price + Decimal("1"),
            low_price=close_price - Decimal("1"),
            close_price=close_price,
            volume_qty=1_000,
            source_name="test-source",
            source_file="prices.csv",
            source_hash="test-source-hash",
            ingested_at=datetime(2024, 1, 22, tzinfo=UTC),
            run_id="price-run",
            dataset_version="test-v1",
        )
        for index, close_price in enumerate(close_values)
    ]

    write_daily_prices_parquet(prices, output_directory)


def write_historical_source_file(path: Path) -> None:
    with gzip.open(path, mode="wt", encoding="utf-8", newline="") as file:
        file.write(
            "Date,Open,High,Low,Close,Volume,Dividends,Stock Splits\n"
            "2024-01-02 00:00:00+05:30,"
            "2500.00,2530.00,2490.00,2515.00,1000,0.0,0.0\n"
        )


def write_bhavcopy_source_file(path: Path) -> None:
    with gzip.open(path, mode="wt", encoding="utf-8", newline="") as file:
        file.write(
            "SYMBOL,SERIES,DATE1,PREV_CLOSE,OPEN_PRICE,HIGH_PRICE,"
            "LOW_PRICE,LAST_PRICE,CLOSE_PRICE,AVG_PRICE,TTL_TRD_QNTY,"
            "TURNOVER_LACS,NO_OF_TRADES,DELIV_QTY,DELIV_PER\n"
            "RELIANCE,EQ,01-Mar-2024,2900.00,2900.00,2930.00,2890.00,"
            "2915.40,2915.40,2910.00,1000,29.10,50,600,60.00\n"
        )


def test_load_and_query_sample_prices(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("QDP_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("QDP_DATABASE_PATH", str(tmp_path / "qdp.duckdb"))
    get_settings.cache_clear()

    fixture_path = Path(__file__).parents[1] / "fixtures" / "v0" / "daily_prices.csv"

    load_result = runner.invoke(app, ["load-csv", str(fixture_path)])

    assert load_result.exit_code == 0
    assert "daily_prices.parquet" in load_result.output

    query_result = runner.invoke(app, ["query", "--symbol", "RELIANCE"])

    assert query_result.exit_code == 0
    assert "RELIANCE" in query_result.output
    assert "TCS" not in query_result.output

    get_settings.cache_clear()


def test_load_csv_writes_parquet_via_cli(tmp_path: Path, monkeypatch) -> None:
    data_dir = configure_test_settings(monkeypatch, tmp_path)

    source = tmp_path / "daily_prices.csv"
    source.write_text(
        "exchange,symbol,trade_date,open_price,high_price,low_price,"
        "close_price,volume_qty,source_name,source_file,ingested_at,"
        "run_id,dataset_version\n"
        "NSE,reliance,2026-01-02,1400.00,1425.00,1390.00,1415.00,"
        "1000000,test_source,prices.csv,2026-01-02T12:00:00+00:00,"
        "test-run,test-v1\n",
        encoding="utf-8",
    )

    result = runner.invoke(app, ["load-csv", str(source)])

    assert result.exit_code == 0, result.output
    assert "Wrote" in result.output
    assert len(list((data_dir / "daily_prices").rglob("*.parquet"))) == 1


def test_inventory_historical_writes_report_via_cli(tmp_path: Path) -> None:
    source_directory = tmp_path / "source"
    source_directory.mkdir()
    write_historical_source_file(source_directory / "RELIANCE.NS.csv.gz")

    output_path = tmp_path / "reports" / "inventory.json"

    result = runner.invoke(
        app,
        [
            "inventory-historical",
            str(source_directory),
            "--run-id",
            "test-run",
            "--dataset-version",
            "kaggle-2023-11",
            "--output-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Files discovered: 1" in result.output
    assert "Files processed: 1" in result.output
    assert output_path.exists()


def test_stage_historical_stages_data_via_cli(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_dir = configure_test_settings(monkeypatch, tmp_path)

    source_directory = tmp_path / "source"
    source_directory.mkdir()
    write_historical_source_file(source_directory / "RELIANCE.NS.csv.gz")

    result = runner.invoke(
        app,
        [
            "stage-historical",
            str(source_directory),
            "--run-id",
            "test-run",
            "--dataset-version",
            "kaggle-2023-11",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Files discovered: 1" in result.output
    assert "Files processed: 1" in result.output
    assert len(list(data_dir.rglob("*.parquet"))) == 1


def test_stage_bhavcopy_stages_prices_via_cli(
    tmp_path: Path,
    monkeypatch,
) -> None:

    data_dir = configure_test_settings(monkeypatch, tmp_path)

    source_directory = tmp_path / "source"
    source_directory.mkdir()
    write_bhavcopy_source_file(source_directory / "sec_bhavdata_full_01032024.csv.gz")

    result = runner.invoke(
        app,
        [
            "stage-bhavcopy",
            str(source_directory),
            "--run-id",
            "test-run",
            "--dataset-version",
            "nse-bhavcopy-v1",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Files discovered: 1" in result.output
    assert "Files processed: 1" in result.output
    assert len(list((data_dir / "daily_prices").rglob("*.parquet"))) == 1


def test_recommendation_preview_via_cli(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_dir = configure_test_settings(monkeypatch, tmp_path)
    daily_prices_directory = data_dir / "daily_prices"

    close_values = [Decimal(100)] + [Decimal(101)] * 19 + [Decimal(121)]
    start_date = date(2024, 1, 1)

    prices = [
        DailyPrice(
            exchange="NSE",
            symbol="RELIANCE",
            trade_date=start_date + timedelta(days=index),
            open_price=close_price,
            high_price=close_price + Decimal(1),
            low_price=close_price - Decimal(1),
            close_price=close_price,
            volume_qty=1_000,
            source_name="test-source",
            source_file="prices.csv",
            source_hash="test-source-hash",
            ingested_at=datetime(2024, 1, 1, tzinfo=UTC),
            run_id="test-run",
            dataset_version="test-v1",
        )
        for index, close_price in enumerate(close_values)
    ]

    write_daily_prices_parquet(prices, daily_prices_directory)

    result = runner.invoke(
        app,
        [
            "recommendation-preview",
            "reliance",
            "--account-id",
            "main",
            "--run-id",
            "test-run",
            "--input-dataset-version",
            "test-v1",
            "--as-of",
            "2024-01-21",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "main | NSE | RELIANCE | BUY" in result.output
    assert "2024-01-21" in result.output
    assert "121.00000000" in result.output


def test_recommendation_preview_rejects_invalid_as_of() -> None:
    result = runner.invoke(
        app,
        [
            "recommendation-preview",
            "reliance",
            "--account-id",
            "main",
            "--run-id",
            "test-run",
            "--input-dataset-version",
            "test-v1",
            "--as-of",
            "not-a-date",
        ],
    )

    assert result.exit_code != 0
    assert "Use ISO format YYYY-MM-DD." in result.output


def test_recommendation_record_via_cli(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_dir = configure_test_settings(monkeypatch, tmp_path)
    write_recording_test_prices(data_dir / "daily_prices")

    result = runner.invoke(
        app,
        [
            "recommendation-record",
            "reliance",
            "--account-id",
            "main",
            "--run-id",
            "test-run",
            "--input-dataset-version",
            "test-v1",
            "--as-of",
            "2024-01-21",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "main | NSE | RELIANCE | BUY" in result.output
    assert "Recorded recommendation |" in result.output

    ledger_paths = list((data_dir / "recommendations").rglob("*.parquet"))
    assert len(ledger_paths) == 1

    records = pq.read_table(ledger_paths[0]).to_pylist()
    assert records[0]["symbol"] == "RELIANCE"
    assert records[0]["action"] == "BUY"
    assert records[0]["run_id"] == "test-run"
    assert records[0]["recommendation_id"] in result.output


def test_recommendation_record_rejects_duplicate_run(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_dir = configure_test_settings(monkeypatch, tmp_path)
    write_recording_test_prices(data_dir / "daily_prices")

    arguments = [
        "recommendation-record",
        "RELIANCE",
        "--account-id",
        "main",
        "--run-id",
        "test-run",
        "--input-dataset-version",
        "test-v1",
        "--as-of",
        "2024-01-21",
    ]

    first_result = runner.invoke(app, arguments)
    assert first_result.exit_code == 0, first_result.output

    ledger_paths = list((data_dir / "recommendations").rglob("*.parquet"))
    assert len(ledger_paths) == 1
    original_bytes = ledger_paths[0].read_bytes()

    second_result = runner.invoke(app, arguments)

    assert second_result.exit_code != 0
    assert "already exists" in second_result.output
    assert ledger_paths[0].read_bytes() == original_bytes
    assert len(list((data_dir / "recommendations").rglob("*.parquet"))) == 1
