from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Literal

from core.data.ranking_snapshot_ledger import (
    write_momentum_ranking_snapshot,
)
from core.models.ranking_snapshot import (
    MomentumRankingRun,
    MomentumRankingSnapshotCandidate,
)
from core.services.ranking import rank_momentum_universe
from core.services.universe import calculate_universe_hash


@dataclass(frozen=True)
class RankingSnapshotWriteResult:
    """The persisted artifacts from one momentum ranking run."""

    ranking_run: MomentumRankingRun
    candidates: list[MomentumRankingSnapshotCandidate]
    run_path: Path
    candidate_path: Path


def generate_and_record_momentum_ranking_snapshot(
    *,
    ranking_run_id: str,
    exchange: Literal["NSE"],
    as_of_date: date,
    generated_at: datetime,
    input_dataset_version: str,
    database_path: Path,
    daily_prices_directory: Path,
    output_directory: Path,
    max_price_age_days: int = 7,
    allowed_symbols: set[str] | None = None,
) -> RankingSnapshotWriteResult:
    """Generate and immutably record one point-in-time momentum ranking."""
    ranked_candidates = rank_momentum_universe(
        exchange=exchange,
        as_of_date=as_of_date,
        input_dataset_version=input_dataset_version,
        database_path=database_path,
        daily_prices_directory=daily_prices_directory,
        max_price_age_days=max_price_age_days,
        allowed_symbols=allowed_symbols,
    )

    if not ranked_candidates:
        raise ValueError("No eligible momentum candidates to record.")

    ranking_run = MomentumRankingRun(
        ranking_run_id=ranking_run_id,
        exchange=exchange,
        as_of_date=as_of_date,
        generated_at=generated_at,
        strategy_name="baseline_momentum",
        strategy_version="v1",
        input_dataset_version=input_dataset_version,
        universe_hash=calculate_universe_hash(
            candidate.symbol for candidate in ranked_candidates
        ),
        max_price_age_days=max_price_age_days,
        eligible_candidate_count=len(ranked_candidates),
    )
    snapshot_candidates = [
        MomentumRankingSnapshotCandidate(
            ranking_run_id=ranking_run_id,
            **candidate.model_dump(mode="python"),
        )
        for candidate in ranked_candidates
    ]

    run_path, candidate_path = write_momentum_ranking_snapshot(
        ranking_run,
        snapshot_candidates,
        output_directory,
    )

    return RankingSnapshotWriteResult(
        ranking_run=ranking_run,
        candidates=snapshot_candidates,
        run_path=run_path,
        candidate_path=candidate_path,
    )
