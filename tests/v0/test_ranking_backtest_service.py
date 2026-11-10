from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from core.data.parquet import write_daily_prices_parquet
from core.models.daily_prices import DailyPrice
from core.services.ranking_backtest import run_momentum_ranking_backtest


def make_price(
    *,
    symbol: str,
    trade_date: date,
    close_price: Decimal,
) -> DailyPrice:
    return DailyPrice(
        exchange="NSE",
        symbol=symbol,
        trade_date=trade_date,
        open_price=close_price,
        high_price=close_price + Decimal(1),
        low_price=close_price - Decimal(1),
        close_price=close_price,
        volume_qty=1_000,
        source_name="test-source",
        source_file="prices.csv",
        source_hash="test-source-hash",
        ingested_at=datetime(2024, 1, 1, tzinfo=UTC),
        run_id="test-run",
        dataset_version="test-v1",
    )


def build_trending_prices(start_date: date) -> list[DailyPrice]:
    prices: list[DailyPrice] = []

    for index in range(61):
        trade_date = start_date + timedelta(days=index)

        prices.append(
            make_price(
                symbol="ALPHA",
                trade_date=trade_date,
                close_price=Decimal(100) + Decimal(index),
            )
        )
        prices.append(
            make_price(
                symbol="BETA",
                trade_date=trade_date,
                close_price=Decimal(100) - Decimal(index),
            )
        )

    return prices


def test_runs_momentum_ranking_backtest(
    tmp_path: Path,
) -> None:
    daily_prices_directory = tmp_path / "daily_prices"

    write_daily_prices_parquet(
        build_trending_prices(date(2024, 1, 1)),
        daily_prices_directory,
    )

    result = run_momentum_ranking_backtest(
        exchange="NSE",
        start_date=date(2024, 1, 21),
        end_date=date(2024, 2, 10),
        evaluation_as_of_date=date(2024, 3, 1),
        allowed_symbols={"ALPHA", "BETA"},
        bucket_size=1,
        step_observations=20,
        input_dataset_version="test-v1",
        database_path=tmp_path / "qdp.duckdb",
        daily_prices_directory=daily_prices_directory,
        generated_at=datetime(2024, 3, 2, tzinfo=UTC),
    )

    assert result.attempted_ranking_date_count == 2
    assert len(result.observations) == 2
    assert result.summary is not None
    assert result.summary.completed_ranking_date_count == 2
    assert result.summary.positive_rank_spread_count == 2
    assert result.summary.rank_spread_hit_rate == Decimal(1)
