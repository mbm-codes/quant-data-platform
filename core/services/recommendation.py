# core/services/recommendation.py

from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from core.data.duckdb import connect, register_daily_prices_view
from core.data.price_history import load_recent_closes
from core.models.recommendations import PortfolioRecommendation
from core.strategies.momentum import (
    calculate_momentum_score,
    calculate_twenty_day_momentum,
    select_momentum_action,
)


def preview_momentum_recommendation(
    *,
    account_id: str,
    symbol: str,
    as_of: date | None,
    run_id: str,
    generated_at: datetime,
    input_dataset_version: str,
    database_path: Path,
    daily_prices_directory: Path,
    max_price_age_days: int = 7,
) -> PortfolioRecommendation:
    """Build a deterministic baseline momentum recommendation from daily prices."""
    if max_price_age_days < 0:
        raise ValueError("max_price_age_days cannot be negative.")

    if generated_at.utcoffset() is None:
        raise ValueError("generated_at must include timezone information.")

    india_today = generated_at.astimezone(ZoneInfo("Asia/Kolkata")).date()
    evaluation_date = as_of if as_of is not None else india_today

    connection = connect(database_path)
    try:
        register_daily_prices_view(
            connection,
            daily_prices_directory,
        )

        closing_prices = load_recent_closes(
            connection=connection,
            exchange="NSE",
            symbol=symbol,
            as_of=evaluation_date,
        )

        momentum_return = calculate_twenty_day_momentum(
            [close_price for _, close_price in closing_prices]
        )

        price_date, reference_price = closing_prices[-1]
        price_age_days = (evaluation_date - price_date).days

        if price_age_days > max_price_age_days:
            raise ValueError(
                f"Stale close price: latest price is {price_age_days} days old; "
                f"maximum allowed is {max_price_age_days} days."
            )

        score = calculate_momentum_score(momentum_return)
        action = select_momentum_action(momentum_return)

        normalized_symbol = symbol.strip().upper()
        effective_as_of = as_of if as_of is not None else price_date
        recommendation_id = (
            f"baseline_momentum:v1:{account_id}:"
            f"{normalized_symbol}:{effective_as_of.isoformat()}"
        )

        return PortfolioRecommendation(
            recommendation_id=recommendation_id,
            account_id=account_id,
            action=action,
            as_of_date=effective_as_of,
            currency="INR",
            exchange="NSE",
            generated_at=generated_at,
            input_dataset_version=input_dataset_version,
            run_id=run_id,
            strategy_name="baseline_momentum",
            strategy_version="v1",
            symbol=symbol,
            score=score,
            price_date=price_date,
            reference_price=reference_price,
            rationale=(
                f"20-day momentum return is {momentum_return:.2%}; "
                "action follows the baseline momentum policy."
            ),
        )
    finally:
        connection.close()
