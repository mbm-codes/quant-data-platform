from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from core.data.parquet import write_daily_prices_parquet
from core.data.recommendation_ledger import write_recommendation_ledger
from core.models.daily_prices import DailyPrice
from core.models.recommendation_evaluation import RecommendationEvaluation
from core.models.recommendations import PortfolioRecommendation
from core.services.recommendation_evaluation_recording import (
    evaluate_and_record_recommendation,
    evaluate_and_record_recommendation_by_id,
)


def make_recommendation() -> PortfolioRecommendation:
    return PortfolioRecommendation(
        recommendation_id="rec-001",
        account_id="main",
        exchange="NSE",
        symbol="RELIANCE",
        action="BUY",
        as_of_date=date(2024, 1, 1),
        price_date=date(2024, 1, 1),
        reference_price=Decimal(100),
        score=Decimal("0.75"),
        rationale="Test recommendation.",
        strategy_name="baseline_momentum",
        strategy_version="v1",
        generated_at=datetime(2024, 1, 1, tzinfo=UTC),
        run_id="test-run",
        input_dataset_version="test-v1",
    )


def test_records_completed_evaluation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    completed_evaluation = RecommendationEvaluation(
        recommendation_id="rec-001",
        evaluation_as_of_date=date(2024, 2, 1),
        future_observations_found=20,
        status="COMPLETED",
        future_price_date=date(2024, 1, 21),
        future_close_price=Decimal(110),
        forward_return=Decimal("0.10"),
    )
    output_path = tmp_path / "evaluations" / "evaluation.parquet"

    monkeypatch.setattr(
        "core.services.recommendation_evaluation_recording.evaluate_recommendation",
        lambda *args, **kwargs: completed_evaluation,
    )
    monkeypatch.setattr(
        "core.services.recommendation_evaluation_recording."
        "write_completed_recommendation_evaluations",
        lambda evaluations, output_directory: [output_path],
    )

    evaluation, written_path = evaluate_and_record_recommendation(
        make_recommendation(),
        evaluation_as_of_date=date(2024, 2, 1),
        database_path=tmp_path / "qdp.duckdb",
        daily_prices_directory=tmp_path / "daily_prices",
        evaluation_ledger_directory=tmp_path / "evaluations",
    )

    assert evaluation == completed_evaluation
    assert written_path == output_path


def test_does_not_record_pending_evaluation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pending_evaluation = RecommendationEvaluation(
        recommendation_id="rec-001",
        evaluation_as_of_date=date(2024, 1, 15),
        future_observations_found=12,
        status="PENDING",
    )

    monkeypatch.setattr(
        "core.services.recommendation_evaluation_recording.evaluate_recommendation",
        lambda *args, **kwargs: pending_evaluation,
    )

    def fail_if_called(*args, **kwargs) -> list[Path]:
        raise AssertionError("Pending evaluation must not be written.")

    monkeypatch.setattr(
        "core.services.recommendation_evaluation_recording."
        "write_completed_recommendation_evaluations",
        fail_if_called,
    )

    evaluation, written_path = evaluate_and_record_recommendation(
        make_recommendation(),
        evaluation_as_of_date=date(2024, 1, 15),
        database_path=tmp_path / "qdp.duckdb",
        daily_prices_directory=tmp_path / "daily_prices",
        evaluation_ledger_directory=tmp_path / "evaluations",
    )

    assert evaluation == pending_evaluation
    assert written_path is None


def test_loads_evaluates_and_records_recommendation_by_id(
    tmp_path: Path,
) -> None:
    recommendation = make_recommendation()
    recommendations_directory = tmp_path / "recommendations"
    daily_prices_directory = tmp_path / "daily_prices"
    evaluation_ledger_directory = tmp_path / "evaluations"

    write_recommendation_ledger(
        [recommendation],
        recommendations_directory,
    )

    prices = [
        DailyPrice(
            exchange="NSE",
            symbol="RELIANCE",
            trade_date=date(2024, 1, 1) + timedelta(days=index),
            open_price=Decimal(101) + Decimal(index - 1),
            high_price=Decimal(102) + Decimal(index - 1),
            low_price=Decimal(100) + Decimal(index - 1),
            close_price=Decimal(101) + Decimal(index - 1),
            volume_qty=1_000,
            source_name="test-source",
            source_file="prices.csv",
            source_hash="test-source-hash",
            ingested_at=datetime(2024, 1, 1, tzinfo=UTC),
            run_id="test-run",
            dataset_version="test-v1",
        )
        for index in range(1, 21)
    ]
    prices[-1] = prices[-1].model_copy(
        update={
            "open_price": Decimal(110),
            "high_price": Decimal(110),
            "low_price": Decimal(110),
            "close_price": Decimal(110),
        }
    )
    write_daily_prices_parquet(prices, daily_prices_directory)

    evaluation, written_path = evaluate_and_record_recommendation_by_id(
        "rec-001",
        evaluation_as_of_date=date(2024, 1, 21),
        database_path=tmp_path / "qdp.duckdb",
        recommendations_directory=recommendations_directory,
        daily_prices_directory=daily_prices_directory,
        evaluation_ledger_directory=evaluation_ledger_directory,
    )

    assert evaluation.status == "COMPLETED"
    assert evaluation.forward_return == Decimal("0.10")
    assert written_path is not None
    assert written_path.exists()
