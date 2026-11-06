from datetime import date
from pathlib import Path

from core.data.duckdb import connect, register_daily_prices_view
from core.data.price_history import load_recent_closes_by_symbol
from core.models.ranking import MomentumRankedCandidate
from core.strategies.momentum import rank_momentum_candidates


def rank_momentum_universe(
    *,
    exchange: str,
    as_of_date: date,
    input_dataset_version: str,
    database_path: Path,
    daily_prices_directory: Path,
    max_price_age_days: int = 7,
    allowed_symbols: set[str] | None = None,
) -> list[MomentumRankedCandidate]:
    """Rank fresh, eligible symbols by baseline momentum score."""
    if max_price_age_days < 0:
        raise ValueError("max_price_age_days cannot be negative.")

    normalized_allowed_symbols = (
        {symbol.strip().upper() for symbol in allowed_symbols}
        if allowed_symbols is not None
        else None
    )

    if normalized_allowed_symbols is not None and not normalized_allowed_symbols:
        raise ValueError("allowed_symbols must contain at least one symbol.")

    connection = connect(database_path)
    try:
        register_daily_prices_view(
            connection,
            daily_prices_directory,
        )
        closes_by_symbol = load_recent_closes_by_symbol(
            connection,
            exchange=exchange,
            dataset_version=input_dataset_version,
            as_of_date=as_of_date,
        )
    finally:
        connection.close()

    fresh_closes_by_symbol = {
        symbol: closes
        for symbol, closes in closes_by_symbol.items()
        if len(closes) == 21
        and (as_of_date - closes[-1][0]).days <= max_price_age_days
        and (normalized_allowed_symbols is None or symbol in normalized_allowed_symbols)
    }

    return rank_momentum_candidates(
        exchange=exchange,
        as_of_date=as_of_date,
        input_dataset_version=input_dataset_version,
        closes_by_symbol=fresh_closes_by_symbol,
    )
