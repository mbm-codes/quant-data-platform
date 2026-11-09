from datetime import datetime
from pathlib import Path

from core.data.duckdb import (
    connect,
    register_ranking_candidate_evaluations_view,
)
from core.data.ranking_evaluation_reader import (
    load_completed_ranking_evaluations,
)
from core.models.ranking_evaluation_analytics import (
    MomentumRankingActionSummary,
)
from core.ranking_evaluation_analytics import summarize_ranking_evaluations


def summarize_recorded_ranking_evaluations(
    *,
    ranking_run_id: str,
    database_path: Path,
    evaluations_directory: Path,
    generated_at: datetime,
) -> list[MomentumRankingActionSummary]:
    """Summarize completed outcomes stored for one ranking run."""
    if not any(evaluations_directory.glob("evaluation_year=*/*.parquet")):
        return []

    connection = connect(database_path)

    try:
        register_ranking_candidate_evaluations_view(
            connection,
            evaluations_directory,
        )

        evaluations = load_completed_ranking_evaluations(
            connection,
            ranking_run_id=ranking_run_id,
        )
    finally:
        connection.close()

    if not evaluations:
        return []

    return summarize_ranking_evaluations(
        evaluations,
        generated_at=generated_at,
    )
