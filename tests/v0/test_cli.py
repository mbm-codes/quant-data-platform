# tests/v0/test_cli.py

import gzip
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pyarrow.parquet as pq
from typer.testing import CliRunner

from core.cli import app
from core.config.settings import get_settings
from core.data.parquet import write_daily_prices_parquet
from core.data.recommendation_evaluation_ledger import (
    write_completed_recommendation_evaluations,
)
from core.data.recommendation_ledger import write_recommendation_ledger
from core.models.daily_prices import DailyPrice
from core.models.ranking import MomentumRankedCandidate
from core.models.ranking_evaluation import MomentumRankingCandidateEvaluation
from core.models.ranking_evaluation_analytics import (
    MomentumRankingActionSummary,
)
from core.models.ranking_snapshot import (
    MomentumRankingRun,
    MomentumRankingSnapshotCandidate,
)
from core.models.recommendation_evaluation import RecommendationEvaluation
from core.models.recommendations import PortfolioRecommendation
from core.services.ranking_evaluation_recording import (
    RankingEvaluationRecordingResult,
)
from core.services.ranking_bucket_analytics import (
    RankingBucketAnalysisResult,
)
from core.services.ranking_snapshot import RankingSnapshotWriteResult
from core.models.ranking_bucket_analytics import (
    MomentumRankingBucketSummary,
)

runner = CliRunner()


def configure_test_settings(monkeypatch, tmp_path: Path) -> Path:
    data_dir = tmp_path / "data"

    monkeypatch.setattr(
        "core.cli.get_settings",
        lambda: SimpleNamespace(
            data_dir=data_dir,
            database_path=tmp_path / "qdp.duckdb",
        ),
    )

    return data_dir


def write_recording_test_prices(output_directory: Path) -> None:
    start_date = date(2024, 1, 1)
    close_values = [Decimal(100)] + [Decimal(101)] * 19 + [Decimal(121)]

    prices = [
        DailyPrice(
            exchange="NSE",
            symbol="RELIANCE",
            trade_date=start_date + timedelta(days=index),
            open_price=close_price,
            high_price=close_price + Decimal(1),
            low_price=close_price - Decimal(1),
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


def write_historical_source_file(path: Path) -> None:
    with gzip.open(path, mode="wt", encoding="utf-8", newline="") as file:
        file.write(
            "Date,Open,High,Low,Close,Volume,Dividends,Stock Splits\n"
            "2024-01-02 00:00:00+05:30,"
            "2500.00,2530.00,2490.00,2515.00,1000,0.0,0.0\n"
        )


def write_bhavcopy_source_file(path: Path) -> None:
    with gzip.open(path, mode="wt", encoding="utf-8", newline="") as file:
        file.write(
            "SYMBOL,SERIES,DATE1,PREV_CLOSE,OPEN_PRICE,HIGH_PRICE,"
            "LOW_PRICE,LAST_PRICE,CLOSE_PRICE,AVG_PRICE,TTL_TRD_QNTY,"
            "TURNOVER_LACS,NO_OF_TRADES,DELIV_QTY,DELIV_PER\n"
            "RELIANCE,EQ,01-Mar-2024,2900.00,2900.00,2930.00,2890.00,"
            "2915.40,2915.40,2910.00,1000,29.10,50,600,60.00\n"
        )


def test_load_and_query_sample_prices(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("QDP_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("QDP_DATABASE_PATH", str(tmp_path / "qdp.duckdb"))
    get_settings.cache_clear()

    fixture_path = Path(__file__).parents[1] / "fixtures" / "v0" / "daily_prices.csv"

    load_result = runner.invoke(app, ["load-csv", str(fixture_path)])

    assert load_result.exit_code == 0
    assert "daily_prices.parquet" in load_result.output

    query_result = runner.invoke(app, ["query", "--symbol", "RELIANCE"])

    assert query_result.exit_code == 0
    assert "RELIANCE" in query_result.output
    assert "TCS" not in query_result.output

    get_settings.cache_clear()


def test_load_csv_writes_parquet_via_cli(tmp_path: Path, monkeypatch) -> None:
    data_dir = configure_test_settings(monkeypatch, tmp_path)

    source = tmp_path / "daily_prices.csv"
    source.write_text(
        "exchange,symbol,trade_date,open_price,high_price,low_price,"
        "close_price,volume_qty,source_name,source_file,ingested_at,"
        "run_id,dataset_version\n"
        "NSE,reliance,2026-01-02,1400.00,1425.00,1390.00,1415.00,"
        "1000000,test_source,prices.csv,2026-01-02T12:00:00+00:00,"
        "test-run,test-v1\n",
        encoding="utf-8",
    )

    result = runner.invoke(app, ["load-csv", str(source)])

    assert result.exit_code == 0, result.output
    assert "Wrote" in result.output
    assert len(list((data_dir / "daily_prices").rglob("*.parquet"))) == 1


def test_inventory_historical_writes_report_via_cli(tmp_path: Path) -> None:
    source_directory = tmp_path / "source"
    source_directory.mkdir()
    write_historical_source_file(source_directory / "RELIANCE.NS.csv.gz")

    output_path = tmp_path / "reports" / "inventory.json"

    result = runner.invoke(
        app,
        [
            "inventory-historical",
            str(source_directory),
            "--run-id",
            "test-run",
            "--dataset-version",
            "kaggle-2023-11",
            "--output-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Files discovered: 1" in result.output
    assert "Files processed: 1" in result.output
    assert output_path.exists()


def test_stage_historical_stages_data_via_cli(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_dir = configure_test_settings(monkeypatch, tmp_path)

    source_directory = tmp_path / "source"
    source_directory.mkdir()
    write_historical_source_file(source_directory / "RELIANCE.NS.csv.gz")

    result = runner.invoke(
        app,
        [
            "stage-historical",
            str(source_directory),
            "--run-id",
            "test-run",
            "--dataset-version",
            "kaggle-2023-11",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Files discovered: 1" in result.output
    assert "Files processed: 1" in result.output
    assert len(list(data_dir.rglob("*.parquet"))) == 1


def test_stage_bhavcopy_stages_prices_via_cli(
    tmp_path: Path,
    monkeypatch,
) -> None:

    data_dir = configure_test_settings(monkeypatch, tmp_path)

    source_directory = tmp_path / "source"
    source_directory.mkdir()
    write_bhavcopy_source_file(source_directory / "sec_bhavdata_full_01032024.csv.gz")

    result = runner.invoke(
        app,
        [
            "stage-bhavcopy",
            str(source_directory),
            "--run-id",
            "test-run",
            "--dataset-version",
            "nse-bhavcopy-v1",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Files discovered: 1" in result.output
    assert "Files processed: 1" in result.output
    assert len(list((data_dir / "daily_prices").rglob("*.parquet"))) == 1


def test_recommendation_preview_via_cli(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_dir = configure_test_settings(monkeypatch, tmp_path)
    daily_prices_directory = data_dir / "daily_prices"

    close_values = [Decimal(100)] + [Decimal(101)] * 19 + [Decimal(121)]
    start_date = date(2024, 1, 1)

    prices = [
        DailyPrice(
            exchange="NSE",
            symbol="RELIANCE",
            trade_date=start_date + timedelta(days=index),
            open_price=close_price,
            high_price=close_price + Decimal(1),
            low_price=close_price - Decimal(1),
            close_price=close_price,
            volume_qty=1_000,
            source_name="test-source",
            source_file="prices.csv",
            source_hash="test-source-hash",
            ingested_at=datetime(2024, 1, 1, tzinfo=UTC),
            run_id="test-run",
            dataset_version="test-v1",
        )
        for index, close_price in enumerate(close_values)
    ]

    write_daily_prices_parquet(prices, daily_prices_directory)

    result = runner.invoke(
        app,
        [
            "recommendation-preview",
            "reliance",
            "--account-id",
            "main",
            "--run-id",
            "test-run",
            "--input-dataset-version",
            "test-v1",
            "--as-of",
            "2024-01-21",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "main | NSE | RELIANCE | BUY" in result.output
    assert "2024-01-21" in result.output
    assert "121.00000000" in result.output


def test_recommendation_preview_rejects_invalid_as_of() -> None:
    result = runner.invoke(
        app,
        [
            "recommendation-preview",
            "reliance",
            "--account-id",
            "main",
            "--run-id",
            "test-run",
            "--input-dataset-version",
            "test-v1",
            "--as-of",
            "not-a-date",
        ],
    )

    assert result.exit_code != 0
    assert "Use ISO format YYYY-MM-DD." in result.output


def test_recommendation_record_via_cli(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_dir = configure_test_settings(monkeypatch, tmp_path)
    write_recording_test_prices(data_dir / "daily_prices")

    result = runner.invoke(
        app,
        [
            "recommendation-record",
            "reliance",
            "--account-id",
            "main",
            "--run-id",
            "test-run",
            "--input-dataset-version",
            "test-v1",
            "--as-of",
            "2024-01-21",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "main | NSE | RELIANCE | BUY" in result.output
    assert "Recorded recommendation |" in result.output

    ledger_paths = list((data_dir / "recommendations").rglob("*.parquet"))
    assert len(ledger_paths) == 1

    records = pq.read_table(ledger_paths[0]).to_pylist()
    assert records[0]["symbol"] == "RELIANCE"
    assert records[0]["action"] == "BUY"
    assert records[0]["run_id"] == "test-run"
    assert records[0]["recommendation_id"] in result.output


def test_recommendation_record_rejects_duplicate_run(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_dir = configure_test_settings(monkeypatch, tmp_path)
    write_recording_test_prices(data_dir / "daily_prices")

    arguments = [
        "recommendation-record",
        "RELIANCE",
        "--account-id",
        "main",
        "--run-id",
        "test-run",
        "--input-dataset-version",
        "test-v1",
        "--as-of",
        "2024-01-21",
    ]

    first_result = runner.invoke(app, arguments)
    assert first_result.exit_code == 0, first_result.output

    ledger_paths = list((data_dir / "recommendations").rglob("*.parquet"))
    assert len(ledger_paths) == 1
    original_bytes = ledger_paths[0].read_bytes()

    second_result = runner.invoke(app, arguments)

    assert second_result.exit_code != 0
    assert "already exists" in second_result.output
    assert ledger_paths[0].read_bytes() == original_bytes
    assert len(list((data_dir / "recommendations").rglob("*.parquet"))) == 1


def test_recommendation_evaluate_prints_pending_when_no_future_prices_exist(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_dir = configure_test_settings(monkeypatch, tmp_path)

    recommendation = PortfolioRecommendation(
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
    write_recommendation_ledger(
        [recommendation],
        data_dir / "recommendations",
    )

    reference_price = DailyPrice(
        exchange="NSE",
        symbol="RELIANCE",
        trade_date=date(2024, 1, 1),
        open_price=Decimal(100),
        high_price=Decimal(100),
        low_price=Decimal(100),
        close_price=Decimal(100),
        volume_qty=1_000,
        source_name="test-source",
        source_file="prices.csv",
        source_hash="test-source-hash",
        ingested_at=datetime(2024, 1, 1, tzinfo=UTC),
        run_id="test-run",
        dataset_version="test-v1",
    )
    write_daily_prices_parquet(
        [reference_price],
        data_dir / "daily_prices",
    )

    result = runner.invoke(
        app,
        [
            "recommendation-evaluate",
            "rec-001",
            "--as-of",
            "2024-02-01",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Recommendation ID | rec-001" in result.output
    assert "Evaluation status | PENDING" in result.output
    assert "Future observations | 0" in result.output
    assert not list((data_dir / "recommendation_evaluations").rglob("*.parquet"))


def test_recommendation_evaluate_records_completed_evaluation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_dir = configure_test_settings(monkeypatch, tmp_path)

    recommendation = PortfolioRecommendation(
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
    write_recommendation_ledger(
        [recommendation],
        data_dir / "recommendations",
    )

    future_prices = [
        DailyPrice(
            exchange="NSE",
            symbol="RELIANCE",
            trade_date=date(2024, 1, 1) + timedelta(days=index),
            open_price=Decimal(110),
            high_price=Decimal(110),
            low_price=Decimal(110),
            close_price=Decimal(110),
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
    write_daily_prices_parquet(
        future_prices,
        data_dir / "daily_prices",
    )

    result = runner.invoke(
        app,
        [
            "recommendation-evaluate",
            "rec-001",
            "--as-of",
            "2024-01-21",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Evaluation status | COMPLETED" in result.output
    assert "Future observations | 20" in result.output
    assert "Future price date | 2024-01-21" in result.output
    assert "Future close price | 110.00000000" in result.output
    assert "Forward return | 10.00%" in result.output
    assert "Recorded evaluation |" in result.output
    assert len(list((data_dir / "recommendation_evaluations").rglob("*.parquet"))) == 1


def test_evaluation_summary_prints_empty_message(
    tmp_path: Path,
    monkeypatch,
) -> None:
    configure_test_settings(monkeypatch, tmp_path)

    result = runner.invoke(app, ["evaluation-summary"])

    assert result.exit_code == 0, result.output
    assert result.output == "No completed evaluations.\n"


def test_evaluation_summary_prints_completed_metrics(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_dir = configure_test_settings(monkeypatch, tmp_path)

    recommendations = [
        PortfolioRecommendation(
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
        ),
        PortfolioRecommendation(
            recommendation_id="rec-002",
            account_id="main",
            exchange="NSE",
            symbol="TCS",
            action="BUY",
            as_of_date=date(2024, 1, 2),
            price_date=date(2024, 1, 2),
            reference_price=Decimal(200),
            score=Decimal("0.75"),
            rationale="Test recommendation.",
            strategy_name="baseline_momentum",
            strategy_version="v1",
            generated_at=datetime(2024, 1, 2, tzinfo=UTC),
            run_id="test-run",
            input_dataset_version="test-v1",
        ),
    ]
    evaluations = [
        RecommendationEvaluation(
            recommendation_id="rec-001",
            evaluation_as_of_date=date(2024, 2, 1),
            future_observations_found=20,
            status="COMPLETED",
            future_price_date=date(2024, 1, 21),
            future_close_price=Decimal(110),
            forward_return=Decimal("0.10"),
        ),
        RecommendationEvaluation(
            recommendation_id="rec-002",
            evaluation_as_of_date=date(2024, 2, 2),
            future_observations_found=20,
            status="COMPLETED",
            future_price_date=date(2024, 1, 22),
            future_close_price=Decimal(196),
            forward_return=Decimal("-0.02"),
        ),
    ]
    write_recommendation_ledger(
        recommendations,
        data_dir / "recommendations",
    )
    write_completed_recommendation_evaluations(
        evaluations,
        data_dir / "recommendation_evaluations",
    )

    result = runner.invoke(app, ["evaluation-summary"])

    assert result.exit_code == 0, result.output
    assert "Strategy | Version | Action | Completed" in result.output
    assert "baseline_momentum | v1 | BUY | 2 | 4.00% | 1/2 | 50.00%" in result.output


def test_backtest_momentum_prints_completed_observations(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_dir = configure_test_settings(monkeypatch, tmp_path)
    start_date = date(2024, 1, 1)

    close_values = (
        [Decimal(100)] + [Decimal(101)] * 19 + [Decimal(121)] + [Decimal("133.10")] * 20
    )

    prices = [
        DailyPrice(
            exchange="NSE",
            symbol="RELIANCE",
            trade_date=start_date + timedelta(days=index),
            open_price=close_price,
            high_price=close_price,
            low_price=close_price,
            close_price=close_price,
            volume_qty=1_000,
            source_name="test-source",
            source_file="prices.csv",
            source_hash="test-source-hash",
            ingested_at=datetime(2024, 1, 1, tzinfo=UTC),
            run_id="test-run",
            dataset_version="test-v1",
        )
        for index, close_price in enumerate(close_values)
    ]
    write_daily_prices_parquet(prices, data_dir / "daily_prices")

    result = runner.invoke(
        app,
        [
            "backtest-momentum",
            "reliance",
            "--start",
            "2024-01-21",
            "--end",
            "2024-01-21",
            "--input-dataset-version",
            "test-v1",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "As of | Action | Momentum return" in result.output
    assert "2024-01-21 | BUY | 21.00% | 10.00% | 2024-02-10" in result.output
    assert "Completed observations | 1" in result.output
    assert "Action | Observations | Average forward return" in result.output
    assert "BUY | 1 | 10.00% | 10.00% | 1/1 | 100.00%" in result.output


def test_backtest_momentum_summary_only_hides_observation_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_dir = configure_test_settings(monkeypatch, tmp_path)
    start_date = date(2024, 1, 1)

    close_values = (
        [Decimal(100)] + [Decimal(101)] * 19 + [Decimal(121)] + [Decimal("133.10")] * 20
    )
    prices = [
        DailyPrice(
            exchange="NSE",
            symbol="RELIANCE",
            trade_date=start_date + timedelta(days=index),
            open_price=close_price,
            high_price=close_price,
            low_price=close_price,
            close_price=close_price,
            volume_qty=1_000,
            source_name="test-source",
            source_file="prices.csv",
            source_hash="test-source-hash",
            ingested_at=datetime(2024, 1, 1, tzinfo=UTC),
            run_id="test-run",
            dataset_version="test-v1",
        )
        for index, close_price in enumerate(close_values)
    ]
    write_daily_prices_parquet(prices, data_dir / "daily_prices")

    result = runner.invoke(
        app,
        [
            "backtest-momentum",
            "RELIANCE",
            "--start",
            "2024-01-21",
            "--end",
            "2024-01-21",
            "--input-dataset-version",
            "test-v1",
            "--summary-only",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "As of | Action | Momentum return" not in result.output
    assert "Completed observations |" not in result.output
    assert "Action | Observations | Average forward return" in result.output
    assert "BUY | 1 | 10.00% | 10.00% | 1/1 | 100.00%" in result.output

    def test_backtest_momentum_rejects_non_positive_step() -> None:
        result = runner.invoke(
            app,
            [
                "backtest-momentum",
                "RELIANCE",
                "--start",
                "2024-01-01",
                "--end",
                "2024-01-31",
                "--step-observations",
                "0",
            ],
        )

        assert result.exit_code != 0
        assert "step_observations must be greater than zero" in result.output


def test_rank_momentum_uses_symbols_file(
    tmp_path: Path,
    monkeypatch,
) -> None:
    configure_test_settings(monkeypatch, tmp_path)

    symbols_file = tmp_path / "universe.csv"
    symbols_file.write_text(
        "symbol\nreliance\nTCS\n",
        encoding="utf-8",
    )

    captured_arguments: dict[str, object] = {}

    def fake_rank_momentum_universe(**kwargs) -> list[MomentumRankedCandidate]:
        captured_arguments.update(kwargs)

        return [
            MomentumRankedCandidate(
                rank=1,
                exchange="NSE",
                symbol="RELIANCE",
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
        ]

    monkeypatch.setattr(
        "core.cli.rank_momentum_universe",
        fake_rank_momentum_universe,
    )

    result = runner.invoke(
        app,
        [
            "rank-momentum",
            "--as-of",
            "2024-01-21",
            "--symbols-file",
            str(symbols_file),
            "--input-dataset-version",
            "test-v1",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured_arguments["allowed_symbols"] == {"RELIANCE", "TCS"}
    assert "Highest momentum scores" in result.output
    assert "1 | RELIANCE | 2024-01-21 | 20.00% | 100.00% | BUY" in result.output
    assert "Lowest momentum scores" not in result.output


def test_rank_momentum_record_writes_snapshot_via_cli(
    tmp_path: Path,
    monkeypatch,
) -> None:
    configure_test_settings(monkeypatch, tmp_path)

    symbols_file = tmp_path / "universe.csv"
    symbols_file.write_text(
        "symbol\nRELIANCE\n",
        encoding="utf-8",
    )

    captured_arguments: dict[str, object] = {}

    def fake_record_snapshot(**kwargs) -> RankingSnapshotWriteResult:
        captured_arguments.update(kwargs)

        ranking_run = MomentumRankingRun(
            ranking_run_id=kwargs["ranking_run_id"],
            exchange="NSE",
            as_of_date=date(2024, 1, 21),
            generated_at=datetime(2024, 1, 21, 12, tzinfo=UTC),
            strategy_name="baseline_momentum",
            strategy_version="v1",
            input_dataset_version="test-v1",
            universe_hash="a" * 64,
            max_price_age_days=7,
            eligible_candidate_count=1,
        )
        candidate = MomentumRankingSnapshotCandidate(
            ranking_run_id=ranking_run.ranking_run_id,
            rank=1,
            exchange="NSE",
            symbol="RELIANCE",
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

        return RankingSnapshotWriteResult(
            ranking_run=ranking_run,
            candidates=[candidate],
            run_path=tmp_path / "run.parquet",
            candidate_path=tmp_path / "candidates.parquet",
        )

    monkeypatch.setattr(
        "core.cli.generate_and_record_momentum_ranking_snapshot",
        fake_record_snapshot,
    )

    result = runner.invoke(
        app,
        [
            "rank-momentum-record",
            "--as-of",
            "2024-01-21",
            "--run-id",
            "test-run",
            "--symbols-file",
            str(symbols_file),
            "--input-dataset-version",
            "test-v1",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured_arguments["allowed_symbols"] == {"RELIANCE"}
    assert (
        captured_arguments["ranking_run_id"]
        == "baseline_momentum:v1:NSE:2024-01-21:test-run"
    )
    assert "Eligible candidates | 1" in result.output
    assert "Recorded ranking run |" in result.output
    assert "Recorded ranking candidates |" in result.output


def test_evaluates_and_records_ranking_snapshot_via_cli(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_dir = configure_test_settings(monkeypatch, tmp_path)
    captured_arguments: dict[str, object] = {}

    evaluation = MomentumRankingCandidateEvaluation(
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

    def fake_evaluate_and_record(**kwargs) -> RankingEvaluationRecordingResult:
        captured_arguments.update(kwargs)

        return RankingEvaluationRecordingResult(
            evaluations=[evaluation],
            written_paths=[
                data_dir
                / "ranking_evaluations"
                / "evaluation_year=2024"
                / "example.parquet"
            ],
            completed_count=1,
            pending_count=0,
        )

    monkeypatch.setattr(
        "core.cli.evaluate_and_record_momentum_ranking_snapshot",
        fake_evaluate_and_record,
        raising=False,
    )

    result = runner.invoke(
        app,
        [
            "rank-momentum-evaluate",
            "ranking-run-001",
            "--as-of",
            "2024-02-01",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured_arguments["ranking_run_id"] == "ranking-run-001"
    assert captured_arguments["evaluation_as_of_date"] == date(2024, 2, 1)
    assert captured_arguments["ranking_snapshots_directory"] == (
        data_dir / "ranking_snapshots"
    )
    assert captured_arguments["evaluations_directory"] == (
        data_dir / "ranking_evaluations"
    )
    assert "Completed evaluations | 1" in result.output
    assert "Pending evaluations | 0" in result.output
    assert "RELIANCE | BUY | COMPLETED | 10.00%" in result.output


def test_prints_ranking_evaluation_summary_via_cli(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_dir = configure_test_settings(monkeypatch, tmp_path)
    captured_arguments: dict[str, object] = {}

    buy_summary = MomentumRankingActionSummary(
        ranking_run_id="ranking-run-001",
        input_dataset_version="test-v1",
        action="BUY",
        completed_count=2,
        average_forward_return=Decimal("0.04"),
        median_forward_return=Decimal("0.04"),
        directional_correct_count=1,
        directional_hit_rate=Decimal("0.5"),
        evaluation_as_of_date=date(2024, 2, 1),
        generated_at=datetime(2024, 2, 2, tzinfo=UTC),
    )

    def fake_summarize_recorded(**kwargs) -> list[MomentumRankingActionSummary]:
        captured_arguments.update(kwargs)
        return [buy_summary]

    monkeypatch.setattr(
        "core.cli.summarize_recorded_ranking_evaluations",
        fake_summarize_recorded,
        raising=False,
    )

    result = runner.invoke(
        app,
        [
            "ranking-evaluation-summary",
            "ranking-run-001",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured_arguments["ranking_run_id"] == "ranking-run-001"
    assert captured_arguments["database_path"] == tmp_path / "qdp.duckdb"
    assert captured_arguments["evaluations_directory"] == (
        data_dir / "ranking_evaluations"
    )
    assert "Ranking run ID | ranking-run-001" in result.output
    assert "BUY | 2 | 4.00% | 4.00% | 1/2 | 50.00%" in result.output


def test_prints_no_completed_ranking_evaluations_via_cli(
    tmp_path: Path,
    monkeypatch,
) -> None:
    configure_test_settings(monkeypatch, tmp_path)

    monkeypatch.setattr(
        "core.cli.summarize_recorded_ranking_evaluations",
        lambda **kwargs: [],
        raising=False,
    )

    result = runner.invoke(
        app,
        [
            "ranking-evaluation-summary",
            "ranking-run-without-outcomes",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "No completed ranking evaluations." in result.output


def test_prints_ranking_bucket_summary_via_cli(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_dir = configure_test_settings(monkeypatch, tmp_path)
    captured_arguments: dict[str, object] = {}

    top_summary = MomentumRankingBucketSummary(
        ranking_run_id="ranking-run-001",
        input_dataset_version="test-v1",
        bucket="TOP",
        bucket_size=2,
        rank_start=1,
        rank_end=2,
        completed_count=2,
        average_forward_return=Decimal("0.06"),
        median_forward_return=Decimal("0.06"),
        positive_return_count=2,
        positive_return_rate=Decimal("1"),
        evaluation_as_of_date=date(2024, 2, 1),
        generated_at=datetime(2024, 2, 2, tzinfo=UTC),
    )
    bottom_summary = MomentumRankingBucketSummary(
        ranking_run_id="ranking-run-001",
        input_dataset_version="test-v1",
        bucket="BOTTOM",
        bucket_size=2,
        rank_start=5,
        rank_end=6,
        completed_count=2,
        average_forward_return=Decimal("-0.03"),
        median_forward_return=Decimal("-0.03"),
        positive_return_count=0,
        positive_return_rate=Decimal("0"),
        evaluation_as_of_date=date(2024, 2, 1),
        generated_at=datetime(2024, 2, 2, tzinfo=UTC),
    )

    def fake_summarize_recorded_buckets(
        **kwargs,
    ) -> RankingBucketAnalysisResult:
        captured_arguments.update(kwargs)
        return RankingBucketAnalysisResult(
            summaries=[top_summary, bottom_summary],
            average_forward_return_spread=Decimal("0.09"),
        )

    monkeypatch.setattr(
        "core.cli.summarize_recorded_ranking_buckets",
        fake_summarize_recorded_buckets,
        raising=False,
    )

    result = runner.invoke(
        app,
        [
            "ranking-bucket-summary",
            "ranking-run-001",
            "--bucket-size",
            "2",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured_arguments["ranking_run_id"] == "ranking-run-001"
    assert captured_arguments["bucket_size"] == 2
    assert captured_arguments["evaluations_directory"] == (
        data_dir / "ranking_evaluations"
    )
    assert "Ranking run ID | ranking-run-001" in result.output
    assert "Bucket size | 2" in result.output
    assert "TOP | 1-2 | 2 | 6.00% | 6.00% | 2/2 | 100.00%" in result.output
    assert "Average rank spread | 9.00%" in result.output

def test_prints_no_completed_ranking_bucket_evaluations_via_cli(
    tmp_path: Path,
    monkeypatch,
) -> None:
    configure_test_settings(monkeypatch, tmp_path)

    monkeypatch.setattr(
        "core.cli.summarize_recorded_ranking_buckets",
        lambda **kwargs: RankingBucketAnalysisResult(
            summaries=[],
            average_forward_return_spread=None,
        ),
        raising=False,
    )

    result = runner.invoke(
        app,
        [
            "ranking-bucket-summary",
            "ranking-run-without-outcomes",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "No completed ranking evaluations." in result.output