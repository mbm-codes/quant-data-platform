from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from core.data.duckdb import connect, register_recommendations_view
from core.data.recommendation_ledger import write_recommendation_ledger
from core.models.recommendations import PortfolioRecommendation


def make_recommendation(
    recommendation_id: str = "rec-001",
) -> PortfolioRecommendation:
    return PortfolioRecommendation.model_validate(
        {
            "recommendation_id": recommendation_id,
            "account_id": "main",
            "symbol": "RELIANCE",
            "action": "BUY",
            "as_of_date": date(2024, 1, 5),
            "price_date": date(2024, 1, 5),
            "reference_price": Decimal("2607.70"),
            "score": Decimal("0.82"),
            "rationale": "Positive deterministic baseline signal.",
            "strategy_name": "baseline_momentum",
            "strategy_version": "v1",
            "generated_at": datetime.now(UTC),
            "run_id": "test-run",
            "input_dataset_version": "nse-bhavcopy-v1",
        }
    )


def test_writes_immutable_recommendation_record(tmp_path: Path) -> None:
    recommendation = make_recommendation()

    written_paths = write_recommendation_ledger(
        [recommendation],
        tmp_path,
    )

    assert len(written_paths) == 1
    assert written_paths[0].parent == tmp_path / "as_of_year=2024"
    assert written_paths[0].suffix == ".parquet"

    table = pq.read_table(written_paths[0])
    assert table.num_rows == 1
    assert table.column("symbol").to_pylist() == ["RELIANCE"]

    with pytest.raises(ValueError, match="already exists"):
        write_recommendation_ledger([recommendation], tmp_path)


def test_queries_recommendation_ledger_with_duckdb(tmp_path: Path) -> None:
    write_recommendation_ledger(
        [make_recommendation()],
        tmp_path / "recommendations",
    )

    connection = connect(tmp_path / "qdp.duckdb")
    register_recommendations_view(
        connection,
        tmp_path / "recommendations",
    )

    rows = connection.execute(
        """
        SELECT symbol, action, score, as_of_year
        FROM recommendations
        """
    ).fetchall()

    connection.close()

    assert rows == [
        ("RELIANCE", "BUY", Decimal("0.82"), 2024),
    ]
