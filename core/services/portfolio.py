# core/services.portfolio.py

from datetime import date, datetime
from pathlib import Path

from core.data.duckdb import connect, register_daily_prices_view
from core.data.transactions import load_portfolio_transactions_csv
from core.models.portfolio import PortfolioPosition
from core.models.valuation import PortfolioValuation
from core.portfolio import calculate_positions
from core.valuation import value_positions


def get_portfolio_positions(
    source: Path,
    run_id: str,
    ingested_at: datetime,
) -> list[PortfolioPosition]:
    transactions = load_portfolio_transactions_csv(
        source, run_id=run_id, ingested_at=ingested_at
    )
    positions = calculate_positions(transactions)

    return positions


def value_portfolio(
    source: Path,
    run_id: str,
    ingested_at: datetime,
    as_of: date,
    max_price_age_days: int,
    database_path: Path,
    daily_prices_directory: Path,
) -> list[PortfolioValuation]:

    positions = get_portfolio_positions(
        source=source,
        run_id=run_id,
        ingested_at=ingested_at,
    )

    if not positions:
        return []

    connection = connect(database_path)

    try:
        register_daily_prices_view(
            connection,
            daily_prices_directory,
        )
        valuations = value_positions(
            connection,
            positions,
            as_of=as_of,
            max_price_age_days=max_price_age_days,
        )
        return valuations
    finally:
        connection.close()
