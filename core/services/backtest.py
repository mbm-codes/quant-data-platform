from datetime import date
from pathlib import Path

from core.data.duckdb import connect, register_daily_prices_view
from core.data.price_history import load_symbol_closes
from core.models.backtest import MomentumBacktestObservation
from core.strategies.momentum import (
    build_momentum_backtest_observations,
)


def backtest_momentum(
    *,
    exchange: str,
    symbol: str,
    start_date: date,
    end_date: date,
    input_dataset_version: str,
    database_path: Path,
    daily_prices_directory: Path,
    step_observations: int = 1,
) -> list[MomentumBacktestObservation]:
    """Run a read-only baseline momentum backtest for one symbol."""
    if step_observations <= 0:
        raise ValueError("step_observations must be greater than zero.")

    connection = connect(database_path)
    try:
        register_daily_prices_view(
            connection,
            daily_prices_directory,
        )
        closes = load_symbol_closes(
            connection,
            exchange=exchange,
            symbol=symbol,
            dataset_version=input_dataset_version,
        )
    finally:
        connection.close()

    return build_momentum_backtest_observations(
        exchange=exchange,
        symbol=symbol,
        input_dataset_version=input_dataset_version,
        closes=closes,
        start_date=start_date,
        end_date=end_date,
        step_observations=step_observations,
    )
