from datetime import datetime
from typing import Literal

import duckdb

from core.models.daily_price_coverage import DailyPriceCoverage


def load_daily_price_coverage(
    connection: duckdb.DuckDBPyConnection,
    *,
    exchange: Literal["NSE"],
    dataset_version: str,
    generated_at: datetime,
) -> DailyPriceCoverage:
    """Profile canonical daily-price coverage for one source dataset version."""
    row = connection.execute(
        """
        WITH filtered_prices AS (
            SELECT exchange, symbol, trade_date
            FROM daily_prices
            WHERE exchange = ?
              AND dataset_version = ?
        ),
        duplicate_business_keys AS (
            SELECT exchange, symbol, trade_date
            FROM filtered_prices
            GROUP BY exchange, symbol, trade_date
            HAVING COUNT(*) > 1
        )
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT symbol) AS distinct_symbols,
            MIN(trade_date) AS first_trade_date,
            MAX(trade_date) AS last_trade_date,
            (
                SELECT COUNT(*)
                FROM duplicate_business_keys
            ) AS duplicate_business_key_count
        FROM filtered_prices
        """,
        [exchange, dataset_version],
    ).fetchone()

    if row is None:
        raise RuntimeError("Daily-price coverage query returned no result.")

    (
        total_rows,
        distinct_symbols,
        first_trade_date,
        last_trade_date,
        duplicate_business_key_count,
    ) = row

    return DailyPriceCoverage(
        exchange=exchange,
        dataset_version=dataset_version,
        total_rows=total_rows,
        distinct_symbols=distinct_symbols,
        first_trade_date=first_trade_date,
        last_trade_date=last_trade_date,
        duplicate_business_key_count=duplicate_business_key_count,
        generated_at=generated_at,
    )