from dataclasses import dataclass
from datetime import date
from pathlib import Path

from core.data.ranking_evaluation_ledger import (
    write_completed_ranking_candidate_evaluations,
)
from core.models.ranking_evaluation import MomentumRankingCandidateEvaluation
from core.services.ranking_evaluation import (
    evaluate_momentum_ranking_snapshot,
)


@dataclass(frozen=True)
class RankingEvaluationRecordingResult:
    evaluations: list[MomentumRankingCandidateEvaluation]
    written_paths: list[Path]
    completed_count: int
    pending_count: int


def evaluate_and_record_momentum_ranking_snapshot(
    *,
    ranking_run_id: str,
    evaluation_as_of_date: date,
    database_path: Path,
    ranking_snapshots_directory: Path,
    daily_prices_directory: Path,
    evaluations_directory: Path,
) -> RankingEvaluationRecordingResult:
    """Evaluate a ranking snapshot and persist its completed candidate outcomes."""
    evaluations = evaluate_momentum_ranking_snapshot(
        ranking_run_id=ranking_run_id,
        evaluation_as_of_date=evaluation_as_of_date,
        database_path=database_path,
        ranking_snapshots_directory=ranking_snapshots_directory,
        daily_prices_directory=daily_prices_directory,
    )

    completed_evaluations = [
        evaluation for evaluation in evaluations if evaluation.status == "COMPLETED"
    ]
    pending_count = len(evaluations) - len(completed_evaluations)

    written_paths: list[Path] = []
    if completed_evaluations:
        written_paths = write_completed_ranking_candidate_evaluations(
            completed_evaluations,
            evaluations_directory,
        )

    return RankingEvaluationRecordingResult(
        evaluations=evaluations,
        written_paths=written_paths,
        completed_count=len(completed_evaluations),
        pending_count=pending_count,
    )
