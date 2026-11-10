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


def load_historical_closes_by_symbol(
    connection: duckdb.DuckDBPyConnection,
    *,
    exchange: str,
    dataset_version: str,
    evaluation_as_of_date: date,
    allowed_symbols: set[str] | None,
) -> dict[str, list[tuple[date, Decimal]]]:
    """Load ordered daily closes for a universe up to an evaluation cutoff."""
    if allowed_symbols is not None and not allowed_symbols:
        return {}

    sql = """
        SELECT symbol, trade_date, close_price
        FROM daily_prices
        WHERE exchange = ?
          AND dataset_version = ?
          AND trade_date <= ?
    """
    parameters: list[object] = [
        exchange.strip().upper(),
        dataset_version,
        evaluation_as_of_date,
    ]

    if allowed_symbols is not None:
        normalized_symbols = sorted(
            symbol.strip().upper() for symbol in allowed_symbols
        )
        placeholders = ", ".join("?" for _ in normalized_symbols)
        sql += f" AND symbol IN ({placeholders})"
        parameters.extend(normalized_symbols)

    sql += " ORDER BY symbol, trade_date"

    rows = connection.execute(sql, parameters).fetchall()

    closes_by_symbol: dict[str, list[tuple[date, Decimal]]] = {}

    for symbol, trade_date, close_price in rows:
        closes_by_symbol.setdefault(symbol, []).append((trade_date, close_price))

    return closes_by_symbol

def load_latest_trade_date(
    connection: duckdb.DuckDBPyConnection,
    *,
    exchange: str,
    dataset_version: str,
) -> date | None:
    """Return the newest canonical trade date for one dataset version."""
    row = connection.execute(
        """
        SELECT MAX(trade_date)
        FROM daily_prices
        WHERE exchange = ?
          AND dataset_version = ?
        """,
        [
            exchange.strip().upper(),
            dataset_version,
        ],
    ).fetchone()

    if row is None:
        return None

    return row[0]