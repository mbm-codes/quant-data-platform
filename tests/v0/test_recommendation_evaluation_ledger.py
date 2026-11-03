from datetime import date
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from core.data.recommendation_evaluation_ledger import (
    write_completed_recommendation_evaluations,
)
from core.models.recommendation_evaluation import RecommendationEvaluation


def make_completed_evaluation() -> RecommendationEvaluation:
    return RecommendationEvaluation(
        recommendation_id="rec-001",
        evaluation_as_of_date=date(2024, 2, 15),
        future_observations_found=20,
        status="COMPLETED",
        future_price_date=date(2024, 2, 1),
        future_close_price=Decimal("110.00"),
        forward_return=Decimal("0.10"),
    )


def test_writes_completed_evaluation_to_immutable_ledger(
    tmp_path: Path,
) -> None:
    evaluation = make_completed_evaluation()

    written_paths = write_completed_recommendation_evaluations(
        [evaluation],
        tmp_path,
    )

    assert len(written_paths) == 1
    assert written_paths[0].parent == tmp_path / "evaluation_year=2024"

    table = pq.read_table(written_paths[0])
    assert table.num_rows == 1
    assert table.column("recommendation_id").to_pylist() == ["rec-001"]

    with pytest.raises(ValueError, match="already exists."):
        write_completed_recommendation_evaluations([evaluation], tmp_path)


def test_rejects_pending_evaluation_from_ledger(
    tmp_path: Path,
) -> None:
    pending_evaluation = RecommendationEvaluation(
        recommendation_id="rec-001",
        evaluation_as_of_date=date(2024, 1, 15),
        future_observations_found=12,
        status="PENDING",
    )

    with pytest.raises(ValueError, match="Only completed evaluations"):
        write_completed_recommendation_evaluations(
            [pending_evaluation],
            tmp_path,
        )
