from datetime import UTC, datetime
from pathlib import Path

from core.data.transactions import load_portfolio_transactions_csv


def test_loads_and_normalizes_portfolio_transactions(tmp_path: Path) -> None:
    source_path = tmp_path / "portfolio_transactions.csv"
    source_path.write_text(
        "transaction_id,account_id,symbol,trade_date,side,"
        "quantity,unit_price,fees\n"
        "txn-001,main,reliance,2024-01-02,buy,10,2510.00,20.00\n"
        "txn-002,main,reliance,2024-01-03,sell,4,2520.00,20.00\n",
        encoding="utf-8",
    )

    transactions = load_portfolio_transactions_csv(
        source_path,
        run_id="test-run",
        ingested_at=datetime.now(UTC),
    )

    assert len(transactions) == 2
    assert transactions[0].symbol == "RELIANCE"
    assert transactions[0].side == "BUY"
    assert transactions[0].source_file == "portfolio_transactions.csv"
    assert transactions[1].quantity == 4
