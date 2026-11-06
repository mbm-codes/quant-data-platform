from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from core.data.ranking_snapshot_ledger import (
    write_momentum_ranking_snapshot,
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


def test_writes_immutable_ranking_snapshot(tmp_path: Path) -> None:
    ranking_run = make_ranking_run()
    candidates = [
        make_candidate(1, "RELIANCE"),
        make_candidate(2, "TCS"),
    ]

    run_path, candidate_path = write_momentum_ranking_snapshot(
        ranking_run,
        candidates,
        tmp_path,
    )

    assert run_path.parent == tmp_path / "runs" / "as_of_year=2024"
    assert candidate_path.parent == (tmp_path / "candidates" / "as_of_year=2024")

    run_table = pq.read_table(run_path)
    candidate_table = pq.read_table(candidate_path)

    assert run_table.num_rows == 1
    assert candidate_table.num_rows == 2
    assert candidate_table.column("symbol").to_pylist() == [
        "RELIANCE",
        "TCS",
    ]

    with pytest.raises(ValueError, match="already exists"):
        write_momentum_ranking_snapshot(
            ranking_run,
            candidates,
            tmp_path,
        )


def test_does_not_write_partial_snapshot_when_arrow_validation_fails(
    tmp_path: Path,
) -> None:
    ranking_run = make_ranking_run()
    candidates = [
        make_candidate(1, "RELIANCE"),
        make_candidate(2, "TCS").model_copy(
            update={
                "momentum_return": Decimal("0.123456789"),
            }
        ),
    ]

    with pytest.raises(pa.ArrowInvalid, match="Rescaling Decimal"):
        write_momentum_ranking_snapshot(
            ranking_run,
            candidates,
            tmp_path,
        )

    assert not list(tmp_path.rglob("*.parquet"))
