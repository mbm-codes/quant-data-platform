import duckdb

from core.models.recommendations import PortfolioRecommendation

RECOMMENDATION_COLUMNS = (
    "recommendation_id",
    "account_id",
    "exchange",
    "symbol",
    "action",
    "as_of_date",
    "price_date",
    "reference_price",
    "currency",
    "score",
    "rationale",
    "strategy_name",
    "strategy_version",
    "generated_at",
    "run_id",
    "input_dataset_version",
)


def load_recommendation(
    connection: duckdb.DuckDBPyConnection,
    *,
    recommendation_id: str,
) -> PortfolioRecommendation:
    """Load exactly one recommendation from the registered ledger view."""
    column_sql = ", ".join(RECOMMENDATION_COLUMNS)

    rows = connection.execute(
        f"""
        SELECT {column_sql}
        FROM recommendations
        WHERE recommendation_id = ?
        LIMIT 2 
        """,
        [recommendation_id],
    ).fetchall()

    if not rows:
        raise ValueError(f"Recommendation not found: {recommendation_id}")

    if len(rows) > 1:
        raise ValueError(f"Duplicate recommendation records found: {recommendation_id}")

    return PortfolioRecommendation.model_validate(
        dict(zip(RECOMMENDATION_COLUMNS, rows[0], strict=True))
    )
