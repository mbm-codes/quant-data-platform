from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from core.models.ranking_backtest import MomentumRankingBacktestObservation
from core.ranking_backtest import summarize_momentum_ranking_backtest


def make_observation(
    *,
    ranking_date: date,
    rank_spread: Decimal,
) -> MomentumRankingBacktestObservation:
    return MomentumRankingBacktestObservation(
        strategy_name="baseline_momentum",
        strategy_version="v1",
        input_dataset_version="test-v1",
        universe_hash="a" * 64,
        ranking_date=ranking_date,
        evaluation_as_of_date=ranking_date + timedelta(days=20),
        bucket_size=2,
        top_rank_start=1,
        top_rank_end=2,
        bottom_rank_start=5,
        bottom_rank_end=6,
        top_completed_count=2,
        bottom_completed_count=2,
        top_average_forward_return=rank_spread,
        bottom_average_forward_return=Decimal(0),
        rank_spread=rank_spread,
        top_positive_return_rate=Decimal(1),
        bottom_positive_return_rate=Decimal(0),
        generated_at=datetime(2024, 4, 1, tzinfo=UTC),
    )


def test_summarizes_ranking_backtest_observations() -> None:
    observations = [
        make_observation(
            ranking_date=date(2024, 1, 21),
            rank_spread=Decimal("0.10"),
        ),
        make_observation(
            ranking_date=date(2024, 2, 10),
            rank_spread=Decimal("-0.04"),
        ),
        make_observation(
            ranking_date=date(2024, 3, 1),
            rank_spread=Decimal("0.06"),
        ),
    ]

    summary = summarize_momentum_ranking_backtest(
        observations,
        attempted_ranking_date_count=4,
        start_date=date(2024, 1, 21),
        end_date=date(2024, 3, 1),
        step_observations=20,
        forward_horizon_observations=20,
        generated_at=datetime(2024, 4, 1, tzinfo=UTC),
    )

    assert summary.completed_ranking_date_count == 3
    assert summary.attempted_ranking_date_count == 4
    assert summary.positive_rank_spread_count == 2
    assert summary.rank_spread_hit_rate == Decimal(2) / Decimal(3)
    assert summary.average_rank_spread == Decimal("0.04")
    assert summary.median_rank_spread == Decimal("0.06")
