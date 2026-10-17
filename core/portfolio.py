from collections import defaultdict
from decimal import Decimal
from typing import Literal

from core.models.portfolio import PortfolioPosition
from core.models.transactions import PortfolioTransaction


def calculate_positions(
    transactions: list[PortfolioTransaction],
) -> list[PortfolioPosition]:
    """Calculate active long positions from ordered transaction events."""
    quantities: defaultdict[tuple[str, Literal["NSE"], str], Decimal] = defaultdict(
        Decimal
    )
    transaction_ids: set[str] = set()

    for transaction in sorted(
        transactions,
        key=lambda item: (item.trade_date, item.transaction_id),
    ):
        if transaction.transaction_id in transaction_ids:
            raise ValueError(f"Duplicate transaction ID: {transaction.transaction_id}")

        transaction_ids.add(transaction.transaction_id)
        key: tuple[str, Literal["NSE"], str] = (
            transaction.account_id,
            transaction.exchange,
            transaction.symbol,
        )
        current_quantity = quantities[key]

        if transaction.side == "BUY":
            quantities[key] = current_quantity + transaction.quantity
            continue

        if transaction.quantity > current_quantity:
            raise ValueError(
                f"Cannot sell {transaction.quantity} {transaction.symbol}; "
                f"only {current_quantity} is held."
            )

        quantities[key] = current_quantity - transaction.quantity

    return [
        PortfolioPosition(
            account_id=account_id,
            exchange=exchange,
            symbol=symbol,
            quantity=quantity,
        )
        for (account_id, exchange, symbol), quantity in sorted(quantities.items())
        if quantity > 0
    ]
