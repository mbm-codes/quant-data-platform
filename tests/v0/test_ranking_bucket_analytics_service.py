from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

from core.data.ranking_evaluation_ledger import (
    write_completed_ranking_candidate_evaluations,
)
from core.models.ranking_evaluation import MomentumRankingCandidateEvaluation
from core.services.ranking_bucket_analytics import (
    summarize_recorded_ranking_buckets,
)


def make_completed_evaluation(
    *,
    rank: int,
    symbol: str,
    forward_return: Decimal,
) -> MomentumRankingCandidateEvaluation:
    return MomentumRankingCandidateEvaluation(
        ranking_run_id="ranking-run-001",
        exchange="NSE",
        symbol=symbol,
        rank=rank,
        action="BUY",
        price_date=date(2024, 1, 1),
        reference_price=Decimal("100"),
        input_dataset_version="test-v1",
        evaluation_as_of_date=date(2024, 2, 1),
        future_observations_found=20,
        status="COMPLETED",
        future_price_date=date(2024, 1, 21),
        future_close_price=Decimal("110"),
        forward_return=forward_return,
    )


def test_summarizes_recorded_top_and_bottom_buckets(
    tmp_path: Path,
) -> None:
    evaluations_directory = tmp_path / "ranking_evaluations"

    write_completed_ranking_candidate_evaluations(
        [
            make_completed_evaluation(
                rank=1,
                symbol="ALPHA",
                forward_return=Decimal("0.10"),
            ),
            make_completed_evaluation(
                rank=2,
                symbol="BETA",
                forward_return=Decimal("0.02"),
            ),
            make_completed_evaluation(
                rank=3,
                symbol="GAMMA",
                forward_return=Decimal("0.50"),
            ),
            make_completed_evaluation(
                rank=4,
                symbol="DELTA",
                forward_return=Decimal("-0.50"),
            ),
            make_completed_evaluation(
                rank=5,
                symbol="EPSILON",
                forward_return=Decimal("-0.02"),
            ),
            make_completed_evaluation(
                rank=6,
                symbol="ZETA",
                forward_return=Decimal("-0.04"),
            ),
        ],
        evaluations_directory,
    )

    result = summarize_recorded_ranking_buckets(
        ranking_run_id="ranking-run-001",
        database_path=tmp_path / "qdp.duckdb",
        evaluations_directory=evaluations_directory,
        bucket_size=2,
        generated_at=datetime(2024, 2, 2, tzinfo=UTC),
    )

    assert len(result.summaries) == 2
    assert result.average_forward_return_spread == Decimal("0.09")


    summaries_by_bucket = {
        summary.bucket: summary
        for summary in result.summaries
    }
    assert summaries_by_bucket["TOP"].average_forward_return == Decimal("0.06")
    assert summaries_by_bucket["BOTTOM"].average_forward_return == Decimal(
        "-0.03"
    )


def test_returns_empty_list_when_no_completed_evaluations_exist(
    tmp_path: Path,
) -> None:
    result = summarize_recorded_ranking_buckets(
        ranking_run_id="missing-ranking-run",
        database_path=tmp_path / "qdp.duckdb",
        evaluations_directory=tmp_path / "ranking_evaluations",
        bucket_size=2,
        generated_at=datetime(2024, 2, 2, tzinfo=UTC),
    )

    assert result.summaries == []
    assert result.average_forward_return_spread is None