from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from core.models.ranking_evaluation import MomentumRankingCandidateEvaluation
from core.ranking_bucket_analytics import summarize_ranking_buckets


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


def test_summarizes_top_and_bottom_ranking_buckets() -> None:
    evaluations = [
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
    ]

    summaries = summarize_ranking_buckets(
        evaluations,
        bucket_size=2,
        generated_at=datetime(2024, 2, 2, tzinfo=UTC),
    )

    summaries_by_bucket = {
        summary.bucket: summary
        for summary in summaries
    }

    top = summaries_by_bucket["TOP"]
    assert top.bucket_size == 2
    assert top.rank_start == 1
    assert top.rank_end == 2
    assert top.completed_count == 2
    assert top.average_forward_return == Decimal("0.06")
    assert top.median_forward_return == Decimal("0.06")
    assert top.positive_return_count == 2
    assert top.positive_return_rate == Decimal("1")

    bottom = summaries_by_bucket["BOTTOM"]
    assert bottom.bucket_size == 2
    assert bottom.rank_start == 5
    assert bottom.rank_end == 6
    assert bottom.completed_count == 2
    assert bottom.average_forward_return == Decimal("-0.03")
    assert bottom.median_forward_return == Decimal("-0.03")
    assert bottom.positive_return_count == 0
    assert bottom.positive_return_rate == Decimal("0")

def test_rejects_bucket_size_that_would_overlap_buckets() -> None:
    evaluations = [
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
            forward_return=Decimal("-0.05"),
        ),
    ]

    with pytest.raises(
        ValueError,
        match="At least twice the bucket_size completed evaluations",
    ):
        summarize_ranking_buckets(
            evaluations,
            bucket_size=2,
            generated_at=datetime(2024, 2, 2, tzinfo=UTC),
        )


def test_rejects_non_positive_bucket_size() -> None:
    evaluations = [
        make_completed_evaluation(
            rank=1,
            symbol="ALPHA",
            forward_return=Decimal("0.10"),
        ),
        make_completed_evaluation(
            rank=2,
            symbol="BETA",
            forward_return=Decimal("-0.05"),
        ),
    ]

    with pytest.raises(
        ValueError,
        match="bucket_size must be greater than zero",
    ):
        summarize_ranking_buckets(
            evaluations,
            bucket_size=0,
            generated_at=datetime(2024, 2, 2, tzinfo=UTC),
        )