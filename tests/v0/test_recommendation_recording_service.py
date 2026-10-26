from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from core.models.recommendations import PortfolioRecommendation
from core.services.recommendation_recording import (
    record_recommendation,
    generate_and_record_momentum_recommendation,
)
from core.data.parquet import write_daily_prices_parquet
from core.models.daily_prices import DailyPrice


def make_recommendation() -> PortfolioRecommendation:
    return PortfolioRecommendation(
        recommendation_id=(
            "baseline_momentum:v1:main:NSE:RELIANCE:2024-01-21:test-run"
        ),
        account_id="main",
        exchange="NSE",
        symbol="RELIANCE",
        action="BUY",
        as_of_date=date(2024, 1, 21),
        price_date=date(2024, 1, 21),
        reference_price=Decimal("121.00"),
        currency="INR",
        score=Decimal("1"),
        rationale="20-day momentum return is 21.00%.",
        strategy_name="baseline_momentum",
        strategy_version="v1",
        generated_at=datetime(2024, 1, 22, tzinfo=UTC),
        run_id="test-run",
        input_dataset_version="test-v1",
    )


def write_test_price_history(output_directory: Path) -> None:
    start_date = date(2024, 1, 1)
    close_values = [Decimal("100")] + [Decimal("101")] * 19 + [Decimal("121")]

    prices = [
        DailyPrice(
            exchange="NSE",
            symbol="RELIANCE",
            trade_date=start_date + timedelta(days=index),
            open_price=close_price,
            high_price=close_price + Decimal("1"),
            low_price=close_price - Decimal("1"),
            close_price=close_price,
            volume_qty=1_000,
            source_name="test-source",
            source_file="prices.csv",
            source_hash="test-source-hash",
            ingested_at=datetime(2024, 1, 22, tzinfo=UTC),
            run_id="price-run",
            dataset_version="test-v1",
        )
        for index, close_price in enumerate(close_values)
    ]

    write_daily_prices_parquet(prices, output_directory)


def generate_test_recommendation(
    tmp_path: Path,
) -> tuple[PortfolioRecommendation, Path]:
    return generate_and_record_momentum_recommendation(
        account_id="main",
        symbol="RELIANCE",
        as_of=date(2024, 1, 21),
        run_id="test-run",
        generated_at=datetime(2024, 1, 22, tzinfo=UTC),
        input_dataset_version="test-v1",
        database_path=tmp_path / "qdp.duckdb",
        daily_prices_directory=tmp_path / "daily_prices",
        ledger_directory=tmp_path / "recommendations",
    )


def test_records_recommendation_in_ledger(tmp_path: Path) -> None:
    recommendation = make_recommendation()

    written_path = record_recommendation(
        recommendation,
        tmp_path / "recommendations",
    )

    assert written_path.exists()
    assert written_path.parent == tmp_path / "recommendations" / "as_of_year=2024"

    records = pq.read_table(written_path).to_pylist()

    assert len(records) == 1
    assert records[0]["recommendation_id"] == recommendation.recommendation_id
    assert records[0]["action"] == "BUY"
    assert records[0]["run_id"] == "test-run"
    assert records[0]["input_dataset_version"] == "test-v1"


def test_rejects_duplicate_without_changing_origial(tmp_path: Path) -> None:
    recommendation = make_recommendation()
    ledger_directory = tmp_path / "recommendations"

    written_path = record_recommendation(recommendation, ledger_directory)
    original_bytes = written_path.read_bytes()

    with pytest.raises(ValueError, match="already exists"):
        record_recommendation(recommendation, ledger_directory)

    assert written_path.read_bytes() == original_bytes
    assert len(list(ledger_directory.rglob("*.parquet"))) == 1


def test_generates_and_records_momentum_recommendation(tmp_path: Path) -> None:
    write_test_price_history(tmp_path / "daily_prices")

    recommendation, written_path = generate_test_recommendation(tmp_path)

    assert recommendation.action == "BUY"
    assert written_path.exists()

    records = pq.read_table(written_path).to_pylist()

    assert len(records) == 1
    assert records[0]["recommendation_id"] == recommendation.recommendation_id
    assert records[0]["run_id"] == "test-run"
    assert records[0]["input_dataset_version"] == "test-v1"


def test_rejects_repeated_recording_for_same_run(tmp_path: Path) -> None:
    write_test_price_history(tmp_path / "daily_prices")

    _, written_path = generate_test_recommendation(tmp_path)
    original_bytes = written_path.read_bytes()

    with pytest.raises(ValueError, match="already exists"):
        generate_test_recommendation(tmp_path)

    assert written_path.read_bytes() == original_bytes
