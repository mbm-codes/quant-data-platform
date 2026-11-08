from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from core.data.parquet import write_daily_prices_parquet
from core.data.ranking_snapshot_ledger import write_momentum_ranking_snapshot
from core.models.daily_prices import DailyPrice
from core.models.ranking_snapshot import (
    MomentumRankingRun,
    MomentumRankingSnapshotCandidate,
)
from core.services.ranking_evaluation import (
    evaluate_momentum_ranking_snapshot,
)


def make_price(trade_date: date, close_price: Decimal) -> DailyPrice:
    return DailyPrice(
        exchange="NSE",
        symbol="RELIANCE",
        trade_date=trade_date,
        open_price=close_price,
        high_price=close_price + Decimal(1),
        low_price=close_price - Decimal(1),
        close_price=close_price,
        volume_qty=1_000,
        source_name="test-source",
        source_file=f"prices-{trade_date.isoformat()}.csv",
        source_hash=f"hash-{trade_date.isoformat()}",
        ingested_at=datetime(2024, 1, 1, tzinfo=UTC),
        run_id="test-run",
        dataset_version="test-v1",
    )


def write_snapshot(
    snapshot_directory: Path,
    *,
    ranking_run_id: str = "ranking-run-001",
) -> None:
    ranking_run = MomentumRankingRun(
        ranking_run_id=ranking_run_id,
        exchange="NSE",
        as_of_date=date(2024, 1, 1),
        generated_at=datetime(2024, 1, 1, tzinfo=UTC),
        strategy_name="baseline_momentum",
        strategy_version="v1",
        input_dataset_version="test-v1",
        universe_hash="a" * 64,
        max_price_age_days=7,
        eligible_candidate_count=1,
    )

    candidate = MomentumRankingSnapshotCandidate(
        ranking_run_id=ranking_run_id,
        exchange="NSE",
        rank=1,
        symbol="RELIANCE",
        as_of_date=date(2024, 1, 1),
        price_date=date(2024, 1, 1),
        reference_price=Decimal(100),
        momentum_return=Decimal("0.10"),
        score=Decimal(1),
        action="BUY",
        strategy_name="baseline_momentum",
        strategy_version="v1",
        input_dataset_version="test-v1",
    )

    write_momentum_ranking_snapshot(
        ranking_run,
        [candidate],
        snapshot_directory,
    )


def test_evaluates_completed_ranking_candidate(tmp_path: Path) -> None:
    snapshot_directory = tmp_path / "ranking_snapshots"
    daily_prices_directory = tmp_path / "daily_prices"
    database_path = tmp_path / "qdp.duckdb"

    write_snapshot(snapshot_directory)

    # 20 future observations: Jan 2 through Jan 21.
    prices = [
        make_price(
            date(2024, 1, 2) + timedelta(days=index),
            Decimal(101) + Decimal(index),
        )
        for index in range(20)
    ]
    write_daily_prices_parquet(prices, daily_prices_directory)

    evaluations = evaluate_momentum_ranking_snapshot(
        ranking_run_id="ranking-run-001",
        evaluation_as_of_date=date(2024, 2, 1),
        database_path=database_path,
        ranking_snapshots_directory=snapshot_directory,
        daily_prices_directory=daily_prices_directory,
    )

    assert len(evaluations) == 1

    evaluation = evaluations[0]
    assert evaluation.ranking_run_id == "ranking-run-001"
    assert evaluation.symbol == "RELIANCE"
    assert evaluation.rank == 1
    assert evaluation.status == "COMPLETED"
    assert evaluation.future_observations_found == 20
    assert evaluation.future_price_date == date(2024, 1, 21)
    assert evaluation.future_close_price == Decimal("120.00000000")
    assert evaluation.forward_return == Decimal("0.20000000")


def test_returns_pending_evaluation_when_future_history_is_incomplete(
    tmp_path: Path,
) -> None:
    snapshot_directory = tmp_path / "ranking_snapshots"
    daily_prices_directory = tmp_path / "daily_prices"
    database_path = tmp_path / "qdp.duckdb"

    write_snapshot(snapshot_directory)

    # Only 19 future observations: insufficient for the 20-observation horizon.
    prices = [
        make_price(
            date(2024, 1, 2) + timedelta(days=index),
            Decimal(101) + Decimal(index),
        )
        for index in range(19)
    ]
    write_daily_prices_parquet(prices, daily_prices_directory)

    evaluations = evaluate_momentum_ranking_snapshot(
        ranking_run_id="ranking-run-001",
        evaluation_as_of_date=date(2024, 2, 1),
        database_path=database_path,
        ranking_snapshots_directory=snapshot_directory,
        daily_prices_directory=daily_prices_directory,
    )

    assert len(evaluations) == 1

    evaluation = evaluations[0]
    assert evaluation.status == "PENDING"
    assert evaluation.future_observations_found == 19
    assert evaluation.future_price_date is None
    assert evaluation.future_close_price is None
    assert evaluation.forward_return is None


def test_rejects_evaluation_before_ranking_date(tmp_path: Path) -> None:
    snapshot_directory = tmp_path / "ranking_snapshots"
    daily_prices_directory = tmp_path / "daily_prices"
    database_path = tmp_path / "qdp.duckdb"

    write_snapshot(snapshot_directory)

    # The service registers both snapshot and price views before validating.
    write_daily_prices_parquet(
        [make_price(date(2024, 1, 2), Decimal(101))],
        daily_prices_directory,
    )

    with pytest.raises(
        ValueError,
        match="cannot be before the ranking run date",
    ):
        evaluate_momentum_ranking_snapshot(
            ranking_run_id="ranking-run-001",
            evaluation_as_of_date=date(2023, 12, 31),
            database_path=database_path,
            ranking_snapshots_directory=snapshot_directory,
            daily_prices_directory=daily_prices_directory,
        )
