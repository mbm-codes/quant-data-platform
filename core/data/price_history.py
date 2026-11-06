# core/data/price_history.py

from collections import defaultdict
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


def load_symbol_closes(
    connection: duckdb.DuckDBPyConnection,
    *,
    exchange: str,
    symbol: str,
    dataset_version: str,
) -> list[tuple[date, Decimal]]:
    """Return all unique canonical closes for one symbol in date order."""
    rows = connection.execute(
        """
        SELECT
            trade_date,
            MIN(close_price) AS close_price,
            COUNT(*) AS row_count
        FROM daily_prices
        WHERE exchange = ?
            AND symbol = ?
            AND dataset_version = ?
        GROUP BY trade_date
        ORDER BY trade_date ASC
        """,
        [
            exchange.strip().upper(),
            symbol.strip().upper(),
            dataset_version,
        ],
    ).fetchall()

    for trade_date, _, row_count in rows:
        if row_count > 1:
            raise ValueError(
                f"Duplicate daily prices found for {trade_date.isoformat()}."
            )

    return [(trade_date, close_price) for trade_date, close_price, _ in rows]


def load_recent_closes_by_symbol(
    connection: duckdb.DuckDBPyConnection,
    *,
    exchange: str,
    dataset_version: str,
    as_of_date: date,
    limit: int = 21,
) -> dict[str, list[tuple[date, Decimal]]]:
    """Return up to the latest closes per symbol, ordered by symbol and date."""
    if limit < 0:
        raise ValueError("limit must be greater than zero.")

    rows = connection.execute(
        """
        WITH prices_by_date AS (
            SELECT
                symbol,
                trade_date,
                MIN(close_price) AS close_price,
                COUNT(*) AS row_count
            FROM daily_prices
            WHERE exchange = ?
                AND dataset_version = ?
                AND trade_date <= ?
            GROUP BY symbol, trade_date
        ),
        recent_prices AS (
            SELECT
                symbol,
                trade_date,
                close_price,
                row_count,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date DESC
                ) AS observation_rank
            FROM prices_by_date
        )
        SELECT
            symbol,
            trade_date,
            close_price,
            row_count
        FROM recent_prices
        WHERE observation_rank <= ?
        ORDER BY symbol ASC, trade_date ASC
        """,
        [
            exchange.strip().upper(),
            dataset_version,
            as_of_date,
            limit,
        ],
    ).fetchall()

    closes_by_symbol: dict[str, list[tuple[date, Decimal]]] = defaultdict(list)

    for symbol, trade_date, close_price, row_count in rows:
        if row_count > 1:
            raise ValueError(
                f"Duplicate daily prices found for{symbol} on {trade_date.isoformat()}."
            )

        closes_by_symbol[symbol].append((trade_date, close_price))

    return dict(closes_by_symbol)
