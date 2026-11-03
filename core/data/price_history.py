# core/data/price_history.py

from datetime import date
from decimal import Decimal

import duckdb


def load_recent_closes(
    connection: duckdb.DuckDBPyConnection,
    *,
    exchange: str,
    symbol: str,
    dataset_version: str,
    as_of: date | None,
    limit: int = 21,
) -> list[tuple[date, Decimal]]:
    """Return the latest available closing prices in chronological order."""

    sql = """
        SELECT trade_date, close_price
        FROM daily_prices
        WHERE exchange = ?
            AND symbol = ?
            AND dataset_version = ?
    """
    parameters: list[object] = [
        exchange.strip().upper(),
        symbol.strip().upper(),
        dataset_version,
    ]

    if as_of is not None:
        sql += " AND trade_date <= ?"
        parameters.append(as_of)

    sql += " ORDER BY trade_date DESC LIMIT ?"
    parameters.append(limit)

    rows = connection.execute(sql, parameters).fetchall()

    return list(reversed(rows))


def load_future_closes(
    connection: duckdb.DuckDBPyConnection,
    *,
    exchange: str,
    symbol: str,
    dataset_version: str,
    price_date: date,
    evaluation_as_of_date: date,
    limit: int = 20,
) -> list[tuple[date, Decimal]]:
    """Return future closing prices in chronological order."""
    sql = """
        SELECT 
            trade_date, 
            MIN(close_price) as close_price,
            COUNT(*) as row_count
        FROM daily_prices
        WHERE exchange = ?
            AND symbol = ?
            AND dataset_version = ?
            AND trade_date > ?
            AND trade_date <= ?
        GROUP BY trade_date
        ORDER BY trade_date ASC
        LIMIT ?
    """
    parameters: list[object] = [
        exchange.strip().upper(),
        symbol.strip().upper(),
        dataset_version,
        price_date,
        evaluation_as_of_date,
        limit,
    ]

    rows = connection.execute(sql, parameters).fetchall()

    for trade_date, _, row_count in rows:
        if row_count > 1:
            raise ValueError(
                f"Duplicate daily prices found for {trade_date.isoformat()}."
            )

    return [(trade_date, close_price) for trade_date, close_price, _ in rows]
