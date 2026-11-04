from collections import defaultdict
from decimal import Decimal

from core.models.evaluation_analytics import (
    CompletedEvaluationRow,
    EvaluationSummary,
)


def summarize_completed_evaluations(
    rows: list[CompletedEvaluationRow],
) -> list[EvaluationSummary]:
    """Summarize completed recommendation outcomes by strategy and action."""
    grouped_rows: dict[
        tuple[str, str, str],
        list[CompletedEvaluationRow],
    ] = defaultdict(list)

    for row in rows:
        key = (
            row.strategy_name,
            row.strategy_version,
            row.action,
        )
        grouped_rows[key].append(row)

    summaries: list[EvaluationSummary] = []

    for (
        strategy_name,
        strategy_version,
        action,
    ), grouped_evaluations in sorted(grouped_rows.items()):
        completed_count = len(grouped_evaluations)
        total_return = sum(
            (evaluation.forward_return for evaluation in grouped_evaluations),
            start=Decimal(0),
        )
        average_forward_return = total_return / Decimal(completed_count)

        directional_observations, directional_correct_count = _directional_results(
            action, grouped_evaluations
        )

        directional_hit_rate = (
            Decimal(directional_correct_count) / Decimal(directional_observations)
            if directional_observations > 0
            else None
        )

        summaries.append(
            EvaluationSummary(
                strategy_name=strategy_name,
                strategy_version=strategy_version,
                action=action,
                completed_count=completed_count,
                average_forward_return=average_forward_return,
                directional_observations=directional_observations,
                directional_correct_count=directional_correct_count,
                directional_hit_rate=directional_hit_rate,
            )
        )

    return summaries


def _directional_results(
    action: str,
    evaluations: list[CompletedEvaluationRow],
) -> tuple[int, int]:
    if action == "BUY":
        return (
            len(evaluations),
            sum(evaluation.forward_return > 0 for evaluation in evaluations),
        )

    if action == "SELL":
        return (
            len(evaluations),
            sum(evaluation.forward_return < 0 for evaluation in evaluations),
        )

    return 0, 0
