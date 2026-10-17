from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from core.models.transactions import PortfolioTransaction


def valid_transaction() -> dict:
    return {
        "transaction_id": "txn-001",
        "account_id": "main",
        "symbol": "reliance",
        "trade_date": date(2024, 1, 2),
        "side": "buy",
        "quantity": Decimal(10),
        "unit_price": Decimal("2510.00"),
        "fees": Decimal("20.00"),
        "source_file": "portfolio_transactions.csv",
        "ingested_at": datetime.now(UTC),
        "run_id": "test-run",
    }


def test_normalizes_transaction_symbol_and_side() -> None:
    transaction = PortfolioTransaction.model_validate(valid_transaction())

    assert transaction.symbol == "RELIANCE"
    assert transaction.side == "BUY"
    assert transaction.exchange == "NSE"
    assert transaction.currency == "INR"


def test_rejects_non_positive_transaction_quantity() -> None:
    transaction = valid_transaction()
    transaction["quantity"] = Decimal(0)

    with pytest.raises(ValidationError, match="quantity"):
        PortfolioTransaction.model_validate(transaction)


def test_rejects_negative_transaction_fees() -> None:
    transaction = valid_transaction()
    transaction["fees"] = Decimal("-1.00")

    with pytest.raises(ValidationError, match="fees"):
        PortfolioTransaction.model_validate(transaction)
