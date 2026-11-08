from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from core.data.duckdb import (
    connect,
    register_momentum_ranking_snapshot_views,
)
from core.data.ranking_snapshot_ledger import (
    write_momentum_ranking_snapshot,
)
from core.data.ranking_snapshot_reader import (
    load_momentum_ranking_snapshot,
)
from core.models.ranking_snapshot import (
    MomentumRankingRun,
    MomentumRankingSnapshotCandidate,
)


def make_ranking_run() -> MomentumRankingRun:
    return MomentumRankingRun(
        ranking_run_id="ranking-run-001",
        exchange="NSE",
        as_of_date=date(2024, 1, 21),
        generated_at=datetime(2024, 1, 21, 12, tzinfo=UTC),
        strategy_name="baseline_momentum",
        strategy_version="v1",
        input_dataset_version="test-v1",
        universe_hash="a" * 64,
        max_price_age_days=7,
        eligible_candidate_count=2,
    )


def make_candidate(
    rank: int,
    symbol: str,
) -> MomentumRankingSnapshotCandidate:
    return MomentumRankingSnapshotCandidate(
        ranking_run_id="ranking-run-001",
        rank=rank,
        exchange="NSE",
        symbol=symbol,
        as_of_date=date(2024, 1, 21),
        price_date=date(2024, 1, 21),
        reference_price=Decimal(120),
        momentum_return=Decimal("0.20"),
        score=Decimal(1),
        action="BUY",
        strategy_name="baseline_momentum",
        strategy_version="v1",
        input_dataset_version="test-v1",
    )


def test_loads_recorded_ranking_snapshot(tmp_path: Path) -> None:
    snapshot_directory = tmp_path / "ranking_snapshots"
    write_momentum_ranking_snapshot(
        make_ranking_run(),
        [
            make_candidate(1, "RELIANCE"),
            make_candidate(2, "TCS"),
        ],
        snapshot_directory,
    )

    connection = connect(tmp_path / "qdp.duckdb")
    try:
        register_momentum_ranking_snapshot_views(
            connection,
            snapshot_directory,
        )
        snapshot = load_momentum_ranking_snapshot(
            connection,
            ranking_run_id="ranking-run-001",
        )
    finally:
        connection.close()

    assert snapshot.ranking_run.ranking_run_id == "ranking-run-001"
    assert [candidate.symbol for candidate in snapshot.candidates] == [
        "RELIANCE",
        "TCS",
    ]


def test_rejects_missing_ranking_snapshot(tmp_path: Path) -> None:
    snapshot_directory = tmp_path / "ranking_snapshots"
    write_momentum_ranking_snapshot(
        make_ranking_run(),
        [
            make_candidate(1, "RELIANCE"),
            make_candidate(2, "TCS"),
        ],
        snapshot_directory,
    )

    connection = connect(tmp_path / "qdp.duckdb")
    try:
        register_momentum_ranking_snapshot_views(
            connection,
            snapshot_directory,
        )

        with pytest.raises(ValueError, match="Ranking run not found"):
            load_momentum_ranking_snapshot(
                connection,
                ranking_run_id="missing-run",
            )
    finally:
        connection.close()
