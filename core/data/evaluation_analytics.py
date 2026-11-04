import duckdb

from core.models.evaluation_analytics import CompletedEvaluationRow


def load_completed_evaluation_rows(
    connection: duckdb.DuckDBPyConnection,
) -> list[CompletedEvaluationRow]:
    """Load completed evaluations joined to their recorded recommendations."""
    rows = connection.execute(
        """
        SELECT
            evaluation.recommendation_id,
            recommendation.account_id,
            recommendation.strategy_name,
            recommendation.strategy_version,
            recommendation.action,
            recommendation.symbol,
            recommendation.as_of_date,
            evaluation.future_price_date,
            evaluation.forward_return
        FROM recommendation_evaluations AS evaluation
        INNER JOIN recommendations AS recommendation
            ON evaluation.recommendation_id = recommendation.recommendation_id
        WHERE evaluation.status = 'COMPLETED'
        ORDER BY
            evaluation.future_price_date,
            evaluation.recommendation_id
        """
    ).fetchall()

    return [
        CompletedEvaluationRow(
            recommendation_id=recommendation_id,
            account_id=account_id,
            strategy_name=strategy_name,
            strategy_version=strategy_version,
            action=action,
            symbol=symbol,
            as_of_date=as_of_date,
            future_price_date=future_price_date,
            forward_return=forward_return,
        )
        for (
            recommendation_id,
            account_id,
            strategy_name,
            strategy_version,
            action,
            symbol,
            as_of_date,
            future_price_date,
            forward_return,
        ) in rows
    ]
