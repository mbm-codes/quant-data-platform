from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from core.ranking_backtest import (
    build_momentum_ranking_backtest_observations,
)


def build_trending_history(
    *,
    start_date: date,
    starting_price: Decimal,
    daily_change: Decimal,
    observation_count: int,
) -> list[tuple[date, Decimal]]:
    return [
        (
            start_date + timedelta(days=index),
            starting_price + (daily_change * Decimal(index)),
        )
        for index in range(observation_count)
    ]


def test_builds_observations_for_multiple_ranking_dates() -> None:
    start_date = date(2024, 1, 1)

    closes_by_symbol = {
        "ALPHA": build_trending_history(
            start_date=start_date,
            starting_price=Decimal(100),
            daily_change=Decimal(1),
            observation_count=61,
        ),
        "BETA": build_trending_history(
            start_date=start_date,
            starting_price=Decimal(100),
            daily_change=Decimal(-1),
            observation_count=61,
        ),
    }

    result = build_momentum_ranking_backtest_observations(
        closes_by_symbol,
        start_date=date(2024, 1, 21),
        end_date=date(2024, 2, 10),
        evaluation_as_of_date=date(2024, 3, 1),
        bucket_size=1,
        step_observations=20,
        input_dataset_version="test-v1",
        universe_hash="a" * 64,
        generated_at=datetime(2024, 3, 2, tzinfo=UTC),
    )

    assert result.attempted_ranking_date_count == 2
    assert len(result.observations) == 2
    assert [observation.ranking_date for observation in result.observations] == [
        date(2024, 1, 21),
        date(2024, 2, 10),
    ]
    assert all(
        observation.rank_spread > Decimal(0) for observation in result.observations
    )
