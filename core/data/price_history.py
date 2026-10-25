# core/data/price_history.py

from datetime import date
from decimal import Decimal

import duckdb


def load_recent_closes(
    connection: duckdb.DuckDBPyConnection,
    *,
    exchange: str,
    symbol: str,
    as_of: date | None,
    limit: int = 21,
) -> list[tuple[date, Decimal]]:
    """Return the latest available closing prices in chronological order."""

    sql = """
        SELECT trade_date, close_price
        FROM daily_prices
        WHERE exchange = ?
            AND symbol = ?
    """
    parameters: list[object] = [
        exchange.strip().upper(),
        symbol.strip().upper(),
    ]

    if as_of is not None:
        sql += " AND trade_date <= ?"
        parameters.append(as_of)

    sql += " ORDER BY trade_date DESC LIMIT ?"
    parameters.append(limit)

    rows = connection.execute(sql, parameters).fetchall()

    return list(reversed(rows))
