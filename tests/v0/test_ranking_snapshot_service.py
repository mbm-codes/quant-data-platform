from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

from core.models.ranking import MomentumRankedCandidate
from core.services.ranking_snapshot import (
    generate_and_record_momentum_ranking_snapshot,
)


def make_ranked_candidate(
    rank: int,
    symbol: str,
) -> MomentumRankedCandidate:
    return MomentumRankedCandidate(
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


def test_generates_and_records_ranking_snapshot(
    tmp_path: Path,
    monkeypatch,
) -> None:
    ranked_candidates = [
        make_ranked_candidate(1, "RELIANCE"),
        make_ranked_candidate(2, "TCS"),
    ]

    monkeypatch.setattr(
        "core.services.ranking_snapshot.rank_momentum_universe",
        lambda **kwargs: ranked_candidates,
    )

    result = generate_and_record_momentum_ranking_snapshot(
        ranking_run_id="ranking-run-001",
        exchange="NSE",
        as_of_date=date(2024, 1, 21),
        generated_at=datetime(2024, 1, 21, 12, tzinfo=UTC),
        input_dataset_version="test-v1",
        database_path=tmp_path / "qdp.duckdb",
        daily_prices_directory=tmp_path / "daily_prices",
        output_directory=tmp_path / "ranking_snapshots",
        allowed_symbols={"RELIANCE", "TCS"},
    )

    assert result.ranking_run.ranking_run_id == "ranking-run-001"
    assert result.ranking_run.eligible_candidate_count == 2
    assert len(result.ranking_run.universe_hash) == 64
    assert [candidate.symbol for candidate in result.candidates] == [
        "RELIANCE",
        "TCS",
    ]
    assert result.run_path.exists()
    assert result.candidate_path.exists()
