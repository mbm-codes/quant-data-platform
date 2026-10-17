from pathlib import Path

from typer.testing import CliRunner

from core.cli import app

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
