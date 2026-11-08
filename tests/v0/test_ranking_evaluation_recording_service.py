from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from core.data.parquet import write_daily_prices_parquet
from core.data.ranking_snapshot_ledger import write_momentum_ranking_snapshot
from core.models.daily_prices import DailyPrice
from core.models.ranking_snapshot import (
    MomentumRankingRun,
    MomentumRankingSnapshotCandidate,
)
from core.services.ranking_evaluation_recording import (
    evaluate_and_record_momentum_ranking_snapshot,
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


def write_snapshot(snapshot_directory: Path) -> None:
    ranking_run = MomentumRankingRun(
        ranking_run_id="ranking-run-001",
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
        ranking_run_id="ranking-run-001",
        rank=1,
        exchange="NSE",
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


def test_evaluates_and_records_completed_ranking_results(
    tmp_path: Path,
) -> None:
    snapshot_directory = tmp_path / "ranking_snapshots"
    daily_prices_directory = tmp_path / "daily_prices"
    evaluations_directory = tmp_path / "ranking_evaluations"

    write_snapshot(snapshot_directory)

    write_daily_prices_parquet(
        [
            make_price(
                date(2024, 1, 2) + timedelta(days=index),
                Decimal(101) + Decimal(index),
            )
            for index in range(20)
        ],
        daily_prices_directory,
    )

    result = evaluate_and_record_momentum_ranking_snapshot(
        ranking_run_id="ranking-run-001",
        evaluation_as_of_date=date(2024, 2, 1),
        database_path=tmp_path / "qdp.duckdb",
        ranking_snapshots_directory=snapshot_directory,
        daily_prices_directory=daily_prices_directory,
        evaluations_directory=evaluations_directory,
    )

    assert len(result.evaluations) == 1
    assert result.completed_count == 1
    assert result.pending_count == 0
    assert len(result.written_paths) == 1
    assert result.written_paths[0].exists()


def test_does_not_record_pending_ranking_evaluations(
    tmp_path: Path,
) -> None:
    snapshot_directory = tmp_path / "ranking_snapshots"
    daily_prices_directory = tmp_path / "daily_prices"
    evaluations_directory = tmp_path / "ranking_evaluations"

    write_snapshot(snapshot_directory)

    # Only 19 future observations: evaluation remains pending.
    write_daily_prices_parquet(
        [
            make_price(
                date(2024, 1, 2) + timedelta(days=index),
                Decimal(101) + Decimal(index),
            )
            for index in range(19)
        ],
        daily_prices_directory,
    )

    result = evaluate_and_record_momentum_ranking_snapshot(
        ranking_run_id="ranking-run-001",
        evaluation_as_of_date=date(2024, 2, 1),
        database_path=tmp_path / "qdp.duckdb",
        ranking_snapshots_directory=snapshot_directory,
        daily_prices_directory=daily_prices_directory,
        evaluations_directory=evaluations_directory,
    )

    assert result.completed_count == 0
    assert result.pending_count == 1
    assert result.written_paths == []
    assert not evaluations_directory.exists()
