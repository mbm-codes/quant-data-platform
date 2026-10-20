# tests/v0/test_portfolio_cli.py

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from core.cli import app
from core.data.parquet import write_daily_prices_parquet
from core.models.daily_prices import DailyPrice

runner = CliRunner()


def test_prints_active_portfolio_positions(tmp_path: Path) -> None:
    source_path = tmp_path / "portfolio_transactions.csv"
    source_path.write_text(
        "transaction_id,account_id,symbol,trade_date,side,"
        "quantity,unit_price,fees\n"
        "txn-001,main,reliance,2024-01-02,buy,10,2510.00,20.00\n"
        "txn-002,main,reliance,2024-01-03,sell,4,2520.00,20.00\n",
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "portfolio-positions",
            str(source_path),
            "--run-id",
            "test-run",
        ],
    )

    assert result.exit_code == 0
    assert "main | NSE | RELIANCE | 6" in result.stdout


def test_prints_no_active_portfolio_positions(tmp_path: Path) -> None:
    source_path = tmp_path / "portfolio_transactions.csv"
    source_path.write_text(
        "transaction_id,account_id,symbol,trade_date,side,"
        "quantity,unit_price,fees\n"
        "txn-001,main,reliance,2024-01-02,buy,10,2510.00,20.00\n"
        "txn-002,main,reliance,2024-01-03,sell,10,2520.00,20.00\n",
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "portfolio-positions",
            str(source_path),
            "--run-id",
            "test-run",
        ],
    )

    assert result.exit_code == 0
    assert "No active positions" in result.stdout


def test_prints_active_portfolio_value(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source_path = tmp_path / "portfolio_transactions.csv"
    source_path.write_text(
        "transaction_id,account_id,symbol,trade_date,side,"
        "quantity,unit_price,fees\n"
        "txn-001,main,reliance,2024-01-02,buy,10,2510.00,20.00\n"
        "txn-002,main,reliance,2024-01-03,sell,4,2520.00,20.00\n",
        encoding="utf-8",
    )

    data_dir = tmp_path / "data"
    database_path = tmp_path / "qdp.duckdb"
    daily_prices_directory = data_dir / "daily_prices"

    price = DailyPrice(
        exchange="NSE",
        symbol="RELIANCE",
        trade_date=date(2024, 3, 1),
        open_price=Decimal("2900.00"),
        high_price=Decimal("2930.00"),
        low_price=Decimal("2890.00"),
        close_price=Decimal("2915.40"),
        volume_qty=1_000,
        source_name="test_bhavcopy",
        source_file="prices.csv.gz",
        source_hash="test-source-hash",
        ingested_at=datetime(2024, 3, 1, tzinfo=UTC),
        run_id="test-run",
        dataset_version="test-v1",
    )
    write_daily_prices_parquet([price], daily_prices_directory)

    monkeypatch.setattr(
        "core.cli.get_settings",
        lambda: SimpleNamespace(
            database_path=database_path,
            data_dir=data_dir,
        ),
    )

    result = runner.invoke(
        app,
        [
            "portfolio-value",
            str(source_path),
            "--as-of",
            "2024-03-01",
            "--run-id",
            "test-run",
        ],
    )

    assert result.exit_code == 0
    assert "main | NSE | RELIANCE | 6 | 2024-03-01" in result.stdout
    assert "Total market value | 17492.40000000" in result.stdout


def test_prints_no_active_portfolio_value(tmp_path: Path) -> None:
    source_path = tmp_path / "portfolio_transactions.csv"
    source_path.write_text(
        "transaction_id,account_id,symbol,trade_date,side,"
        "quantity,unit_price,fees\n"
        "txn-001,main,reliance,2024-01-02,buy,10,2510.00,20.00\n"
        "txn-002,main,reliance,2024-01-03,sell,10,2520.00,20.00\n",
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "portfolio-value",
            str(source_path),
            "--as-of",
            "2024-03-03",
            "--run-id",
            "test-run",
        ],
    )

    assert result.exit_code == 0
    assert "No active positions" in result.stdout
