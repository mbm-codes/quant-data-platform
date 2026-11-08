from datetime import date
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from core.data.ranking_evaluation_ledger import (
    write_completed_ranking_candidate_evaluations,
)
from core.models.ranking_evaluation import MomentumRankingCandidateEvaluation


def make_completed_evaluation() -> MomentumRankingCandidateEvaluation:
    return MomentumRankingCandidateEvaluation(
        ranking_run_id="ranking-run-001",
        exchange="NSE",
        symbol="RELIANCE",
        rank=1,
        action="BUY",
        price_date=date(2024, 1, 1),
        reference_price=Decimal(100),
        input_dataset_version="test-v1",
        evaluation_as_of_date=date(2024, 2, 1),
        future_observations_found=20,
        status="COMPLETED",
        future_price_date=date(2024, 1, 21),
        future_close_price=Decimal(110),
        forward_return=Decimal("0.10"),
    )


def test_writes_immutable_completed_ranking_evaluation(
    tmp_path: Path,
) -> None:
    evaluation = make_completed_evaluation()

    written_paths = write_completed_ranking_candidate_evaluations(
        [evaluation],
        tmp_path,
    )

    assert len(written_paths) == 1
    assert written_paths[0].parent == tmp_path / "evaluation_year=2024"
    assert written_paths[0].suffix == ".parquet"

    table = pq.read_table(written_paths[0])
    assert table.num_rows == 1
    assert table.column("ranking_run_id").to_pylist() == ["ranking-run-001"]
    assert table.column("symbol").to_pylist() == ["RELIANCE"]

    with pytest.raises(ValueError, match="already exists"):
        write_completed_ranking_candidate_evaluations(
            [evaluation],
            tmp_path,
        )


def test_rejects_pending_ranking_evaluation(tmp_path: Path) -> None:
    pending = make_completed_evaluation().model_copy(
        update={
            "status": "PENDING",
            "future_observations_found": 19,
            "future_price_date": None,
            "future_close_price": None,
            "forward_return": None,
        }
    )

    with pytest.raises(ValueError, match="completed"):
        write_completed_ranking_candidate_evaluations(
            [pending],
            tmp_path,
        )
