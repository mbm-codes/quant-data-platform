from datetime import date
from decimal import Decimal
from pathlib import Path

from core.data.duckdb import (
    connect,
    register_ranking_candidate_evaluations_view,
)
from core.data.ranking_evaluation_ledger import (
    write_completed_ranking_candidate_evaluations,
)
from core.data.ranking_evaluation_reader import (
    load_completed_ranking_evaluations,
)
from core.models.ranking_evaluation import MomentumRankingCandidateEvaluation


def make_completed_evaluation(
    *,
    ranking_run_id: str,
    rank: int,
    symbol: str,
) -> MomentumRankingCandidateEvaluation:
    return MomentumRankingCandidateEvaluation(
        ranking_run_id=ranking_run_id,
        exchange="NSE",
        symbol=symbol,
        rank=rank,
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


def test_loads_completed_evaluations_for_one_ranking_run(
    tmp_path: Path,
) -> None:
    evaluations_directory = tmp_path / "ranking_evaluations"

    write_completed_ranking_candidate_evaluations(
        [
            make_completed_evaluation(
                ranking_run_id="ranking-run-001",
                rank=2,
                symbol="BETA",
            ),
            make_completed_evaluation(
                ranking_run_id="ranking-run-001",
                rank=1,
                symbol="ALPHA",
            ),
            make_completed_evaluation(
                ranking_run_id="ranking-run-002",
                rank=1,
                symbol="GAMMA",
            ),
        ],
        evaluations_directory,
    )

    connection = connect(tmp_path / "qdp.duckdb")
    try:
        register_ranking_candidate_evaluations_view(
            connection,
            evaluations_directory,
        )

        evaluations = load_completed_ranking_evaluations(
            connection,
            ranking_run_id="ranking-run-001",
        )
    finally:
        connection.close()

    assert [evaluation.symbol for evaluation in evaluations] == [
        "ALPHA",
        "BETA",
    ]
    assert [evaluation.rank for evaluation in evaluations] == [1, 2]


def test_returns_empty_list_when_ranking_run_has_no_completed_evaluations(
    tmp_path: Path,
) -> None:
    evaluations_directory = tmp_path / "ranking_evaluations"

    write_completed_ranking_candidate_evaluations(
        [
            make_completed_evaluation(
                ranking_run_id="ranking-run-001",
                rank=1,
                symbol="ALPHA",
            )
        ],
        evaluations_directory,
    )

    connection = connect(tmp_path / "qdp.duckdb")
    try:
        register_ranking_candidate_evaluations_view(
            connection,
            evaluations_directory,
        )

        evaluations = load_completed_ranking_evaluations(
            connection,
            ranking_run_id="missing-ranking-run",
        )
    finally:
        connection.close()

    assert evaluations == []
