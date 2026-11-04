from pathlib import Path

from core.data.duckdb import (
    connect,
    register_recommendation_evaluations_view,
    register_recommendations_view,
)
from core.data.evaluation_analytics import load_completed_evaluation_rows
from core.evaluation_analytics import summarize_completed_evaluations
from core.models.evaluation_analytics import EvaluationSummary


def summarize_recorded_evaluations(
    *,
    database_path: Path,
    recommendations_directory: Path,
    evaluations_directory: Path,
) -> list[EvaluationSummary]:
    """Summarize completed evaluations from the local recommendation ledgers."""
    if not list(evaluations_directory.rglob("*.parquet")):
        return []

    connection = connect(database_path)
    try:
        register_recommendations_view(
            connection,
            recommendations_directory,
        )
        register_recommendation_evaluations_view(
            connection,
            evaluations_directory,
        )

        rows = load_completed_evaluation_rows(connection)
    finally:
        connection.close()

    return summarize_completed_evaluations(rows)
