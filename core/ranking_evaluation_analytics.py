from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from statistics import median
from typing import Literal

from core.models.ranking_evaluation import MomentumRankingCandidateEvaluation
from core.models.ranking_evaluation_analytics import (
    MomentumRankingActionSummary,
)

ACTIONS: tuple[Literal["BUY", "HOLD", "SELL"], ...] = (
    "BUY",
    "HOLD",
    "SELL",
)


def summarize_ranking_evaluations(
    evaluations: list[MomentumRankingCandidateEvaluation],
    *,
    generated_at: datetime,
) -> list[MomentumRankingActionSummary]:
    """Summarize completed outcomes from one momentum-ranking run."""
    if not evaluations:
        raise ValueError("At least one ranking evaluation is required.")

    _validate_shared_metadata(evaluations)

    completed_evaluations = [
        evaluation for evaluation in evaluations if evaluation.status == "COMPLETED"
    ]
    if not completed_evaluations:
        raise ValueError("No completed ranking evaluations are available.")

    first_evaluation = completed_evaluations[0]
    evaluations_by_action: dict[
        Literal["BUY", "HOLD", "SELL"],
        list[MomentumRankingCandidateEvaluation],
    ] = defaultdict(list)

    for evaluation in completed_evaluations:
        evaluations_by_action[evaluation.action].append(evaluation)

    summaries: list[MomentumRankingActionSummary] = []

    for action in ACTIONS:
        action_evaluations = evaluations_by_action[action]
        if not action_evaluations:
            continue

        forward_returns = [
            evaluation.forward_return
            for evaluation in action_evaluations
            if evaluation.forward_return is not None
        ]

        summary_fields: dict[str, object] = {
            "ranking_run_id": first_evaluation.ranking_run_id,
            "input_dataset_version": first_evaluation.input_dataset_version,
            "action": action,
            "completed_count": len(action_evaluations),
            "average_forward_return": (
                sum(forward_returns, start=Decimal(0)) / len(forward_returns)
            ),
            "median_forward_return": median(forward_returns),
            "evaluation_as_of_date": first_evaluation.evaluation_as_of_date,
            "generated_at": generated_at,
        }

        if action == "HOLD":
            summary_fields["directional_correct_count"] = None
            summary_fields["directional_hit_rate"] = None
        else:
            correct_count = sum(
                _is_directionally_correct(action, forward_return)
                for forward_return in forward_returns
            )
            summary_fields["directional_correct_count"] = correct_count
            summary_fields["directional_hit_rate"] = Decimal(correct_count) / Decimal(
                len(forward_returns)
            )

        summaries.append(MomentumRankingActionSummary.model_validate(summary_fields))

    return summaries


def _validate_shared_metadata(
    evaluations: list[MomentumRankingCandidateEvaluation],
) -> None:
    first_evaluation = evaluations[0]

    for evaluation in evaluations[1:]:
        if evaluation.ranking_run_id != first_evaluation.ranking_run_id:
            raise ValueError("All evaluations must belong to one ranking run.")

        if evaluation.input_dataset_version != first_evaluation.input_dataset_version:
            raise ValueError("All evaluations must use one input dataset version.")

        if evaluation.evaluation_as_of_date != first_evaluation.evaluation_as_of_date:
            raise ValueError("All evaluations must use one evaluation as-of date.")


def _is_directionally_correct(
    action: Literal["BUY", "SELL"],
    forward_return: Decimal,
) -> bool:
    if action == "BUY":
        return forward_return > Decimal(0)

    return forward_return < Decimal(0)
