from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from core.models.transactions import PortfolioTransaction
from core.portfolio import calculate_positions


def make_transaction(
    transaction_id: str,
    side: str,
    quantity: str,
    trade_date: date,
) -> PortfolioTransaction:
    return PortfolioTransaction.model_validate(
        {
            "transaction_id": transaction_id,
            "account_id": "main",
            "symbol": "RELIANCE",
            "trade_date": trade_date,
            "side": side,
            "quantity": Decimal(quantity),
            "unit_price": Decimal("2500.00"),
            "fees": Decimal("0.00"),
            "source_file": "portfolio_transactions.csv",
            "ingested_at": datetime.now(UTC),
            "run_id": "test-run",
        }
    )


def test_calculates_remaining_quantity_after_partial_sell() -> None:
    positions = calculate_positions(
        [
            make_transaction("txn-001", "BUY", "10", date(2024, 1, 2)),
            make_transaction("txn-002", "SELL", "4", date(2024, 1, 3)),
        ]
    )

    assert len(positions) == 1
    assert positions[0].symbol == "RELIANCE"
    assert positions[0].quantity == Decimal(6)


def test_rejects_sell_larger_than_current_holding() -> None:
    transactions = [
        make_transaction("txn-001", "BUY", "10", date(2024, 1, 2)),
        make_transaction("txn-002", "SELL", "11", date(2024, 1, 3)),
    ]

    with pytest.raises(ValueError, match="Cannot sell"):
        calculate_positions(transactions)


def test_rejects_duplicate_transaction_ids() -> None:
    transactions = [
        make_transaction("txn-001", "BUY", "10", date(2024, 1, 2)),
        make_transaction("txn-001", "BUY", "5", date(2024, 1, 3)),
    ]

    with pytest.raises(ValueError, match="Duplicate transaction ID"):
        calculate_positions(transactions)
