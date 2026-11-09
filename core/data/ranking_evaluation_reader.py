import duckdb

from core.models.ranking_evaluation import MomentumRankingCandidateEvaluation

RANKING_EVALUATION_COLUMNS = (
    "ranking_run_id",
    "exchange",
    "symbol",
    "rank",
    "action",
    "price_date",
    "reference_price",
    "input_dataset_version",
    "evaluation_as_of_date",
    "future_observations_found",
    "status",
    "future_price_date",
    "future_close_price",
    "forward_return",
)


def load_completed_ranking_evaluations(
    connection: duckdb.DuckDBPyConnection,
    *,
    ranking_run_id: str,
) -> list[MomentumRankingCandidateEvaluation]:
    """Load completed evaluations for one ranking run in rank order."""
    rows = connection.execute(
        """
        SELECT
            ranking_run_id,
            exchange,
            symbol,
            rank,
            action,
            price_date,
            reference_price,
            input_dataset_version,
            evaluation_as_of_date,
             future_observations_found,
            status,
            future_price_date,
            future_close_price,
            forward_return
        FROM ranking_candidate_evaluations
        WHERE ranking_run_id = ?
          AND status = 'COMPLETED'
        ORDER BY rank
        """,
        [ranking_run_id],
    ).fetchall()

    return [
        MomentumRankingCandidateEvaluation.model_validate(
            dict(zip(RANKING_EVALUATION_COLUMNS, row, strict=True))
        )
        for row in rows
    ]
