from datetime import date
from decimal import Decimal

import duckdb

from core.models.portfolio import PortfolioPosition
from core.models.valuation import PortfolioValuation


def value_positions(
    connection: duckdb.DuckDBPyConnection,
    positions: list[PortfolioPosition],
    *,
    as_of: date,
    max_price_age_days: int = 7,
) -> list[PortfolioValuation]:
    """Value positions using the latest available close on or before ``as_of``."""
    if max_price_age_days < 0:
        raise ValueError("max_price_age_days must be non-negative")

    valuations: list[PortfolioValuation] = []

    for position in positions:
        row = connection.execute(
            """
            SELECT close_price, trade_date
            from daily_prices
            WHERE exchange = ?
            AND symbol = ?
            AND trade_date <= ?
            ORDER BY trade_date DESC
            LIMIT 1
            """,
            [position.exchange, position.symbol, as_of],
        ).fetchone()

        if row is None:
            raise ValueError(
                f"No close price for {position.exchange} {position.symbol}"
                f"on or before {as_of}."
            )

        close_price: Decimal = row[0]
        price_date: date = row[1]
        price_age_days = (as_of - price_date).days
        if price_age_days > max_price_age_days:
            raise ValueError(
                f"Stale close price for {position.exchange} {position.symbol}: "
                f"{price_date} is {price_age_days} days before {as_of};"
                f"maximum allowed age is {max_price_age_days} days."
            )

        valuations.append(
            PortfolioValuation.model_validate(
                {
                    "account_id": position.account_id,
                    "exchange": position.exchange,
                    "symbol": position.symbol,
                    "quantity": position.quantity,
                    "price_date": price_date,
                    "close_price": close_price,
                    "market_value": position.quantity * close_price,
                }
            )
        )

    return valuations
