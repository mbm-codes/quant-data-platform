from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime
from pathlib import Path

from core.data.duckdb import (
    connect,
    register_ranking_candidate_evaluations_view,
)
from core.data.ranking_evaluation_reader import (
    load_completed_ranking_evaluations,
)
from core.models.ranking_bucket_analytics import (
    MomentumRankingBucketSummary,
)
from core.ranking_bucket_analytics import summarize_ranking_buckets

@dataclass(frozen=True)
class RankingBucketAnalysisResult:
    summaries: list[MomentumRankingBucketSummary]
    average_forward_return_spread: Decimal | None

def summarize_recorded_ranking_buckets(
    *,
    ranking_run_id: str,
    database_path: Path,
    evaluations_directory: Path,
    bucket_size: int,
    generated_at: datetime,
) -> RankingBucketAnalysisResult:
    """Summarize top and bottom buckets for one recorded ranking run."""
    if not any(evaluations_directory.glob("evaluation_year=*/*.parquet")):
        return RankingBucketAnalysisResult(
            summaries=[],
            average_forward_return_spread=None,
        )

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
        return RankingBucketAnalysisResult(
            summaries=[],
            average_forward_return_spread=None,
        )

    summaries = summarize_ranking_buckets(
        evaluations,
        bucket_size=bucket_size,
        generated_at=generated_at,
    )
    summaries_by_bucket = {
        summary.bucket: summary
        for summary in summaries
    }
    average_return_spread = (
        summaries_by_bucket["TOP"].average_forward_return
        - summaries_by_bucket["BOTTOM"].average_forward_return
    )

    return RankingBucketAnalysisResult(
        summaries=summaries,
        average_forward_return_spread=average_return_spread,
    )