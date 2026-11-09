from datetime import datetime
from decimal import Decimal
from statistics import median
from typing import Literal

from core.models.ranking_bucket_analytics import (
    MomentumRankingBucketSummary,
)
from core.models.ranking_evaluation import MomentumRankingCandidateEvaluation


def summarize_ranking_buckets(
    evaluations: list[MomentumRankingCandidateEvaluation],
    *,
    bucket_size: int,
    generated_at: datetime,
) -> list[MomentumRankingBucketSummary]:
    """Compare completed outcomes from the top and bottom ranking buckets."""
    if bucket_size <= 0:
        raise ValueError("bucket_size must be greater than zero.")

    completed_evaluations = [
        evaluation
        for evaluation in evaluations
        if evaluation.status == "COMPLETED"
    ]
    if not completed_evaluations:
        raise ValueError("No completed ranking evaluations are available.")

    _validate_shared_metadata(completed_evaluations)

    ranked_evaluations = sorted(
        completed_evaluations,
        key=lambda evaluation: evaluation.rank,
    )

    if len(ranked_evaluations) < bucket_size * 2:
        raise ValueError(
            "At least twice the bucket_size completed evaluations are required."
        )

    ranks = [evaluation.rank for evaluation in ranked_evaluations]
    if len(ranks) != len(set(ranks)):
        raise ValueError("Ranking evaluations cannot contain duplicate ranks.")

    top_evaluations = ranked_evaluations[:bucket_size]
    bottom_evaluations = ranked_evaluations[-bucket_size:]

    return [
        _build_bucket_summary(
            bucket="TOP",
            evaluations=top_evaluations,
            bucket_size=bucket_size,
            generated_at=generated_at,
        ),
        _build_bucket_summary(
            bucket="BOTTOM",
            evaluations=bottom_evaluations,
            bucket_size=bucket_size,
            generated_at=generated_at,
        ),
    ]


def _build_bucket_summary(
    *,
    bucket: Literal["TOP", "BOTTOM"],
    evaluations: list[MomentumRankingCandidateEvaluation],
    bucket_size: int,
    generated_at: datetime,
) -> MomentumRankingBucketSummary:
    first_evaluation = evaluations[0]
    forward_returns = [
        evaluation.forward_return
        for evaluation in evaluations
        if evaluation.forward_return is not None
    ]
    positive_return_count = sum(
        forward_return > Decimal(0)
        for forward_return in forward_returns
    )

    return MomentumRankingBucketSummary(
        ranking_run_id=first_evaluation.ranking_run_id,
        input_dataset_version=first_evaluation.input_dataset_version,
        bucket=bucket,
        bucket_size=bucket_size,
        rank_start=evaluations[0].rank,
        rank_end=evaluations[-1].rank,
        completed_count=len(evaluations),
        average_forward_return=(
            sum(forward_returns, start=Decimal(0)) / len(forward_returns)
        ),
        median_forward_return=median(forward_returns),
        positive_return_count=positive_return_count,
        positive_return_rate=(
            Decimal(positive_return_count) / Decimal(len(forward_returns))
        ),
        evaluation_as_of_date=first_evaluation.evaluation_as_of_date,
        generated_at=generated_at,
    )


def _validate_shared_metadata(
    evaluations: list[MomentumRankingCandidateEvaluation],
) -> None:
    first_evaluation = evaluations[0]

    for evaluation in evaluations[1:]:
        if evaluation.ranking_run_id != first_evaluation.ranking_run_id:
            raise ValueError("All evaluations must belong to one ranking run.")

        if (
            evaluation.input_dataset_version
            != first_evaluation.input_dataset_version
        ):
            raise ValueError(
                "All evaluations must use one input dataset version."
            )

        if (
            evaluation.evaluation_as_of_date
            != first_evaluation.evaluation_as_of_date
        ):
            raise ValueError(
                "All evaluations must use one evaluation as-of date."
            )