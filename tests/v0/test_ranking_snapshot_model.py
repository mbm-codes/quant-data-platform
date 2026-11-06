from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from core.models.ranking_snapshot import (
    MomentumRankingRun,
    MomentumRankingSnapshotCandidate,
)


def valid_ranking_run() -> dict[str, object]:
    return {
        "ranking_run_id": "ranking-run-001",
        "exchange": "NSE",
        "as_of_date": date(2024, 1, 21),
        "generated_at": datetime(2024, 1, 21, 12, tzinfo=UTC),
        "strategy_name": "baseline_momentum",
        "strategy_version": "v1",
        "input_dataset_version": "test-v1",
        "universe_hash": "a" * 64,
        "max_price_age_days": 7,
        "eligible_candidate_count": 25,
    }


def test_accepts_valid_ranking_run() -> None:
    ranking_run = MomentumRankingRun.model_validate(valid_ranking_run())

    assert ranking_run.ranking_run_id == "ranking-run-001"
    assert ranking_run.eligible_candidate_count == 25


def test_rejects_ranking_run_with_naive_generated_time() -> None:
    record = valid_ranking_run()
    record["generated_at"] = datetime(  # noqa: DTZ001
        2024,
        1,
        21,
        12,
        tzinfo=None,
    )

    with pytest.raises(ValidationError, match="timezone information"):
        MomentumRankingRun.model_validate(record)


def test_rejects_invalid_universe_hash() -> None:
    record = valid_ranking_run()
    record["universe_hash"] = "not-a-sha256-hash"

    with pytest.raises(ValidationError, match="64"):
        MomentumRankingRun.model_validate(record)


def test_accepts_snapshot_candidate() -> None:
    candidate = MomentumRankingSnapshotCandidate(
        ranking_run_id="ranking-run-001",
        rank=1,
        exchange="NSE",
        symbol="reliance",
        as_of_date=date(2024, 1, 21),
        price_date=date(2024, 1, 21),
        reference_price=Decimal(120),
        momentum_return=Decimal("0.20"),
        score=Decimal(1),
        action="BUY",
        strategy_name="baseline_momentum",
        strategy_version="v1",
        input_dataset_version="test-v1",
    )

    assert candidate.ranking_run_id == "ranking-run-001"
    assert candidate.symbol == "RELIANCE"
