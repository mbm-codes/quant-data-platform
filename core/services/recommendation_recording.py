from pathlib import Path
from datetime import date, datetime

from core.data.recommendation_ledger import write_recommendation_ledger
from core.models.recommendations import PortfolioRecommendation
from core.services.recommendation import preview_momentum_recommendation


def record_recommendation(
    recommendation: PortfolioRecommendation,
    ledger_directory: Path,
) -> Path:
    """Record one recommendation in the immutable ledger."""
    written_paths = write_recommendation_ledger(
        [recommendation],
        ledger_directory,
    )
    return written_paths[0]


def generate_and_record_momentum_recommendation(
    *,
    account_id: str,
    symbol: str,
    as_of: date | None,
    run_id: str,
    generated_at: datetime,
    input_dataset_version: str,
    database_path: Path,
    daily_prices_directory: Path,
    ledger_directory: Path,
    max_price_age_days: int = 7,
) -> tuple[PortfolioRecommendation, Path]:
    """Generate one momentum recommendation and record it in the ledger."""
    recommendation = preview_momentum_recommendation(
        account_id=account_id,
        symbol=symbol,
        as_of=as_of,
        run_id=run_id,
        generated_at=generated_at,
        input_dataset_version=input_dataset_version,
        database_path=database_path,
        daily_prices_directory=daily_prices_directory,
        max_price_age_days=max_price_age_days,
    )

    written_path = record_recommendation(
        recommendation,
        ledger_directory,
    )

    return recommendation, written_path
