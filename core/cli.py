# core/cli.py

import logging
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Annotated

import duckdb
import typer

from core.backtest_analytics import summarize_momentum_backtest
from core.config.settings import get_settings
from core.data.duckdb import connect, register_daily_prices_view
from core.models.pipeline_log_event import PipelineLogEvent
from core.observability.pipeline_logging import emit_pipeline_log_event
from core.services.backtest import backtest_momentum
from core.services.bhavcopy_staging import stage_bhavcopy as stage_bhavcopy_service
from core.services.csv_loader import load_csv as load_csv_service
from core.services.evaluation_analytics import (
    summarize_recorded_evaluations,
)
from core.services.historical_inventory import (
    inventory_historical as inventory_historical_service,
)
from core.services.historical_staging import stage_kaggle_historical_directory
from core.services.pipeline_run_status import get_pipeline_run_status
from core.services.pipeline_runs import (
    complete_pipeline_run,
    fail_pipeline_run,
    start_pipeline_run,
)
from core.services.portfolio import get_portfolio_positions, value_portfolio
from core.services.ranking import rank_momentum_universe
from core.services.ranking_backtest import (
    run_momentum_ranking_backtest,
)
from core.services.ranking_bucket_analytics import (
    summarize_recorded_ranking_buckets,
)
from core.services.ranking_evaluation_analytics import (
    summarize_recorded_ranking_evaluations,
)
from core.services.ranking_evaluation_recording import (
    evaluate_and_record_momentum_ranking_snapshot,
)
from core.services.ranking_snapshot import (
    generate_and_record_momentum_ranking_snapshot,
)
from core.services.recommendation import (
    preview_momentum_recommendation as preview_momentum_recommendation_service,
)
from core.services.recommendation_evaluation_recording import (
    evaluate_and_record_recommendation_by_id,
)
from core.services.recommendation_recording import (
    generate_and_record_momentum_recommendation,
)
from core.services.universe import resolve_symbol_universe

app = typer.Typer(no_args_is_help=True)


@app.command("load-csv")
def load_csv(
    source: Annotated[Path, typer.Argument(exists=True, dir_okay=False)],
) -> None:
    """Validate a CSV file and write it as a canonical parquet."""

    settings = get_settings()
    written_paths = load_csv_service(
        source=source,
        daily_prices_directory=settings.data_dir / "daily_prices",
    )

    for written_path in written_paths:
        typer.echo(f"Wrote {written_path}")


@app.command("inventory-historical")
def inventory_historical(
    source_directory: Annotated[
        Path,
        typer.Argument(exists=True, file_okay=False, dir_okay=True),
    ],
    run_id: Annotated[str, typer.Option()],
    dataset_version: Annotated[str, typer.Option()] = "kaggle-2023-11",
    output_path: Annotated[Path, typer.Option()] = Path(
        "reports/kaggle_historical_inventory.json"
    ),
) -> None:
    """Validate and inventory Kaggle historical source files."""

    inventory = inventory_historical_service(
        source_directory=source_directory,
        run_id=run_id,
        dataset_version=dataset_version,
        output_path=output_path,
        ingested_at=datetime.now(UTC),
    )
    report = inventory.to_dict()

    typer.echo(f"Files discovered: {report['files_discovered']}")
    typer.echo(f"Files processed: {report['files_processed']}")
    typer.echo(f"Files failed: {report['files_failed']}")
    typer.echo(f"Files with rejected rows: {report['files_with_rejected_rows']}")
    typer.echo(f"Total valid rows: {report['total_valid_rows']}")
    typer.echo(f"Total rejected rows: {report['total_rejected_rows']}")
    typer.echo(f"Report written to: {output_path}")


@app.command("stage-historical")
def stage_historical(
    source_directory: Annotated[
        Path,
        typer.Argument(exists=True, file_okay=False, dir_okay=True),
    ],
    run_id: Annotated[str, typer.Option()],
    dataset_version: Annotated[str, typer.Option()] = "kaggle-2023-11",
) -> None:
    """Stage valid Kaggle historical rows and quarantine rejected rows."""
    settings = get_settings()

    result = stage_kaggle_historical_directory(
        source_directory,
        settings.data_dir / "staging" / "kaggle_historical_prices" / f"run_id={run_id}",
        settings.data_dir / "quarantine" / "kaggle_historical_rejections",
        run_id=run_id,
        dataset_version=dataset_version,
        ingested_at=datetime.now(UTC),
    )

    typer.echo(f"Files discovered: {result.files_discovered}")
    typer.echo(f"Files processed: {result.files_processed}")
    typer.echo(f"Files failed: {result.files_failed}")
    typer.echo(f"Valid rows staged: {result.valid_rows}")
    typer.echo(f"Rows quarantined: {result.rejected_rows}")


@app.command("stage-bhavcopy")
def stage_bhavcopy(
    source_directory: Annotated[
        Path,
        typer.Argument(exists=True, file_okay=False, dir_okay=True),
    ],
    run_id: Annotated[str, typer.Option()],
    dataset_version: Annotated[str, typer.Option()] = "nse-bhavcopy-v1",
) -> None:
    """Stage official NSE bhavcopy files as canonical daily prices."""
    settings = get_settings()
    started_at = datetime.now(UTC)
    initial_files_discovered = len(list(source_directory.glob("*.csv.gz")))

    pipeline_run = start_pipeline_run(
        run_id=run_id,
        pipeline_name="stage_bhavcopy",
        input_dataset_version=dataset_version,
        output_directory=settings.data_dir / "pipeline_runs",
        started_at=started_at,
    )
    emit_pipeline_log_event(
        PipelineLogEvent(
            timestamp=started_at,
            level="INFO",
            event="pipeline_started",
            run_id=pipeline_run.run_id,
            pipeline_name=pipeline_run.pipeline_name,
            status=pipeline_run.status,
            files_discovered=0,
            files_processed=0,
            valid_rows=0,
            rejected_rows=0,
        )
    )

    try:
        result = stage_bhavcopy_service(
            source_directory,
            settings.data_dir / "daily_prices",
            run_id=run_id,
            dataset_version=dataset_version,
            ingested_at=datetime.now(UTC),
        )
    except Exception as error:
        failed_pipeline_run = fail_pipeline_run(
            pipeline_run,
            output_directory=settings.data_dir / "pipeline_runs",
            finished_at=datetime.now(UTC),
            error_message=str(error) or error.__class__.__name__,
            files_discovered=initial_files_discovered,
            files_processed=0,
            valid_rows=0,
            rejected_rows=0,
        )
        failed_at = failed_pipeline_run.finished_at
        if failed_at is None:
            raise RuntimeError("Failed pipeline run is missing finished_at.")
        emit_pipeline_log_event(
            PipelineLogEvent(
                timestamp=failed_at,
                level="ERROR",
                event="pipeline_failed",
                run_id=failed_pipeline_run.run_id,
                pipeline_name=failed_pipeline_run.pipeline_name,
                status=failed_pipeline_run.status,
                files_discovered=failed_pipeline_run.files_discovered,
                files_processed=failed_pipeline_run.files_processed,
                valid_rows=failed_pipeline_run.valid_rows,
                rejected_rows=failed_pipeline_run.rejected_rows,
                error_message=failed_pipeline_run.error_message,
            )
        )
        raise

    completed_pipeline_run = complete_pipeline_run(
        pipeline_run,
        output_directory=settings.data_dir / "pipeline_runs",
        finished_at=datetime.now(UTC),
        files_discovered=result.files_discovered,
        files_processed=result.files_processed,
        valid_rows=result.valid_rows,
        rejected_rows=0,
    )
    completion_event = (
        "pipeline_completed"
        if completed_pipeline_run.status == "COMPLETED"
        else "pipeline_completed_with_warnings"
    )
    completion_level = (
        "INFO" if completed_pipeline_run.status == "COMPLETED" else "WARNING"
    )
    completed_at = completed_pipeline_run.finished_at
    if completed_at is None:
        raise RuntimeError("Completed pipeline run is missing finished_at.")

    emit_pipeline_log_event(
        PipelineLogEvent(
            timestamp=completed_at,
            level=completion_level,
            event=completion_event,
            run_id=completed_pipeline_run.run_id,
            pipeline_name=completed_pipeline_run.pipeline_name,
            status=completed_pipeline_run.status,
            files_discovered=completed_pipeline_run.files_discovered,
            files_processed=completed_pipeline_run.files_processed,
            valid_rows=completed_pipeline_run.valid_rows,
            rejected_rows=completed_pipeline_run.rejected_rows,
        )
    )

    typer.echo(f"Files discovered: {result.files_discovered}")
    typer.echo(f"Files processed: {result.files_processed}")
    typer.echo(f"Files failed: {result.files_failed}")
    typer.echo(f"Duplicate files skipped: {result.duplicate_files_skipped}")
    typer.echo(f"Valid rows staged: {result.valid_rows}")
    typer.echo(f"Pipeline run status | {completed_pipeline_run.status}")


@app.command("portfolio-positions")
def portfolio_positions(
    source: Annotated[Path, typer.Argument(exists=True, dir_okay=False)],
    run_id: Annotated[str, typer.Option()],
) -> None:
    """Calculate active portfolio positions from transaction events."""

    positions = get_portfolio_positions(
        source=source, run_id=run_id, ingested_at=datetime.now(UTC)
    )

    if not positions:
        typer.echo("No active positions.")
        return

    for position in positions:
        typer.echo(
            " | ".join(
                [
                    position.account_id,
                    position.exchange,
                    position.symbol,
                    str(position.quantity),
                ]
            )
        )


@app.command("portfolio-value")
def portfolio_value(
    source: Annotated[Path, typer.Argument(exists=True, dir_okay=False)],
    as_of: Annotated[str, typer.Option()],
    run_id: Annotated[str, typer.Option()],
    max_price_age_days: Annotated[int, typer.Option()] = 7,
) -> None:
    """Value active portfolio positions using official daily close prices."""
    ingested_at = datetime.now(UTC)
    try:
        as_of_date = date.fromisoformat(as_of)
    except ValueError as error:
        raise typer.BadParameter(
            "Use ISO format YYYY-MM-DD.",
            param_hint="--as-of",
        ) from error

    settings = get_settings()

    try:
        valuations = value_portfolio(
            source,
            run_id=run_id,
            ingested_at=ingested_at,
            as_of=as_of_date,
            max_price_age_days=max_price_age_days,
            database_path=settings.database_path,
            daily_prices_directory=settings.data_dir / "daily_prices",
        )

        if not valuations:
            typer.echo("No active positions.")
            return

    except duckdb.IOException as error:
        raise typer.BadParameter(
            "No canonical daily-price Parquet data exists yet. "
            "Run stage-bhavcopy first."
        ) from error
    except ValueError as error:
        raise typer.BadParameter(str(error)) from error

    for valuation in valuations:
        typer.echo(
            " | ".join(
                [
                    valuation.account_id,
                    valuation.exchange,
                    valuation.symbol,
                    str(valuation.quantity),
                    valuation.price_date.isoformat(),
                    str(valuation.close_price),
                    str(valuation.market_value),
                ]
            )
        )

    total_market_value = sum(
        (valuation.market_value for valuation in valuations),
        start=Decimal(0),
    )
    typer.echo(f"Total market value | {total_market_value}")


@app.command("recommendation-preview")
def recommendation_preview(
    symbol: Annotated[str, typer.Argument()],
    account_id: Annotated[str, typer.Option()],
    run_id: Annotated[str, typer.Option()],
    as_of: Annotated[str | None, typer.Option()] = None,
    input_dataset_version: Annotated[str, typer.Option()] = "nse-bhavcopy-v1",
) -> None:
    """Provide a preview of recommendation"""

    as_of_date = None
    if as_of is not None:
        try:
            as_of_date = date.fromisoformat(as_of)
        except ValueError as error:
            raise typer.BadParameter(
                "Use ISO format YYYY-MM-DD.",
                param_hint="--as-of",
            ) from error

    settings = get_settings()
    try:
        recommendation = preview_momentum_recommendation_service(
            account_id=account_id,
            symbol=symbol,
            database_path=settings.database_path,
            daily_prices_directory=settings.data_dir / "daily_prices",
            as_of=as_of_date,
            generated_at=datetime.now(UTC),
            input_dataset_version=input_dataset_version,
            run_id=run_id,
        )
    except duckdb.IOException as error:
        raise typer.BadParameter(
            "No canonical daily-price Parquet data exists yet. "
            "Run stage-bhavcopy first."
        ) from error
    except ValueError as error:
        raise typer.BadParameter(str(error)) from error

    typer.echo(
        " | ".join(
            [
                recommendation.account_id,
                "NSE",
                recommendation.symbol,
                recommendation.action,
                recommendation.price_date.isoformat(),
                str(recommendation.reference_price),
                str(recommendation.score),
            ]
        )
    )


@app.command("recommendation-record")
def recommendation_record(
    symbol: Annotated[str, typer.Argument()],
    account_id: Annotated[str, typer.Option()],
    run_id: Annotated[str, typer.Option()],
    as_of: Annotated[str | None, typer.Option()] = None,
    input_dataset_version: Annotated[str, typer.Option()] = "nse-bhavcopy-v1",
) -> None:
    """Generate and record a momentum recommendation."""
    as_of_date = None

    if as_of is not None:
        try:
            as_of_date = date.fromisoformat(as_of)
        except ValueError as error:
            raise typer.BadParameter(
                "Use ISO format YYYY-MM-DD.",
                param_hint="--as-of",
            ) from error

    settings = get_settings()

    try:
        recommendation, written_path = generate_and_record_momentum_recommendation(
            account_id=account_id,
            symbol=symbol,
            as_of=as_of_date,
            run_id=run_id,
            generated_at=datetime.now(UTC),
            input_dataset_version=input_dataset_version,
            database_path=settings.database_path,
            daily_prices_directory=settings.data_dir / "daily_prices",
            ledger_directory=settings.data_dir / "recommendations",
        )
    except duckdb.IOException as error:
        raise typer.BadParameter(
            f"Unable to access DuckDB or daily-price data: {error}"
        ) from error
    except ValueError as error:
        raise typer.BadParameter(str(error)) from error

    typer.echo(
        " | ".join(
            [
                recommendation.account_id,
                recommendation.exchange,
                recommendation.symbol,
                recommendation.action,
                recommendation.price_date.isoformat(),
                str(recommendation.reference_price),
                str(recommendation.score),
            ]
        )
    )
    typer.echo(f"Recommendation ID | {recommendation.recommendation_id}")
    typer.echo(f"Recorded recommendation | {written_path}")


@app.command("recommendation-evaluate")
def recommendation_evaluate(
    recommendation_id: Annotated[str, typer.Argument()],
    as_of: Annotated[str, typer.Option()],
) -> None:
    """Evaluate one recorded recommendation using future daily prices."""
    try:
        evaluation_as_of_date = date.fromisoformat(as_of)
    except ValueError as error:
        raise typer.BadParameter(
            "Use ISO format YYYY-MM-DD.",
            param_hint="--as-of",
        ) from error

    settings = get_settings()

    try:
        evaluation, written_path = evaluate_and_record_recommendation_by_id(
            recommendation_id,
            evaluation_as_of_date=evaluation_as_of_date,
            database_path=settings.database_path,
            recommendations_directory=settings.data_dir / "recommendations",
            daily_prices_directory=settings.data_dir / "daily_prices",
            evaluation_ledger_directory=(
                settings.data_dir / "recommendation_evaluations"
            ),
        )
    except duckdb.IOException as error:
        raise typer.BadParameter(
            "Required recommendation or daily-price Parquet data does not exist."
        ) from error
    except ValueError as error:
        raise typer.BadParameter(str(error)) from error

    typer.echo(f"Recommendation ID | {evaluation.recommendation_id}")
    typer.echo(f"Evaluation status | {evaluation.status}")
    typer.echo(f"Future observations | {evaluation.future_observations_found}")

    if evaluation.status == "PENDING":
        typer.echo(f"Evaluation cutoff | {evaluation.evaluation_as_of_date}")
        return

    typer.echo(f"Future price date | {evaluation.future_price_date}")
    typer.echo(f"Future close price | {evaluation.future_close_price}")
    typer.echo(f"Forward return | {evaluation.forward_return:.2%}")
    typer.echo(f"Recorded evaluation | {written_path}")


@app.command("evaluation-summary")
def evaluation_summary() -> None:
    """Summarize completed recommendation evaluations."""
    settings = get_settings()

    try:
        summaries = summarize_recorded_evaluations(
            database_path=settings.database_path,
            recommendations_directory=settings.data_dir / "recommendations",
            evaluations_directory=(settings.data_dir / "recommendation_evaluations"),
        )
    except duckdb.IOException as error:
        raise typer.BadParameter(
            "Unable to read the recommendation or evaluation ledger."
        ) from error

    if not summaries:
        typer.echo("No completed evaluations.")
        return

    typer.echo(
        "Strategy | Version | Action | Completed | Average return | "
        "Directional result | Hit rate"
    )

    for summary in summaries:
        if summary.directional_hit_rate is None:
            directional_result = "N/A"
            hit_rate = "N/A"
        else:
            directional_result = (
                f"{summary.directional_correct_count}/"
                f"{summary.directional_observations}"
            )
            hit_rate = f"{summary.directional_hit_rate:.2%}"

        typer.echo(
            " | ".join(
                [
                    summary.strategy_name,
                    summary.strategy_version,
                    summary.action,
                    str(summary.completed_count),
                    f"{summary.average_forward_return:.2%}",
                    directional_result,
                    hit_rate,
                ]
            )
        )


@app.command("backtest-momentum")
def backtest_momentum_command(
    symbol: Annotated[str, typer.Argument()],
    start: Annotated[str, typer.Option()],
    end: Annotated[str, typer.Option()],
    input_dataset_version: Annotated[str, typer.Option()] = "nse-bhavcopy-v1",
    summary_only: Annotated[bool, typer.Option("--summary-only")] = False,
    step_observations: Annotated[
        int,
        typer.Option("--step-observations"),
    ] = 1,
) -> None:
    """Run the baseline momentum strategy over a historical date range."""
    try:
        start_date = date.fromisoformat(start)
    except ValueError as error:
        raise typer.BadParameter(
            "Use ISO format YYYY-MM-DD.",
            param_hint="--start",
        ) from error

    try:
        end_date = date.fromisoformat(end)
    except ValueError as error:
        raise typer.BadParameter(
            "Use ISO format YYYY-MM-DD.",
            param_hint="--end",
        ) from error

    settings = get_settings()

    try:
        observations = backtest_momentum(
            exchange="NSE",
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            input_dataset_version=input_dataset_version,
            database_path=settings.database_path,
            daily_prices_directory=settings.data_dir / "daily_prices",
            step_observations=step_observations,
        )
    except duckdb.IOException as error:
        raise typer.BadParameter(
            "No canonical daily-price Parquet data exists yet."
        ) from error
    except ValueError as error:
        raise typer.BadParameter(str(error)) from error

    if not observations:
        typer.echo("No complete backtest windows.")
        return

    if not summary_only:
        typer.echo(
            "As of | Action | Momentum return | Forward return | Future price date"
        )

        for observation in observations:
            typer.echo(
                " | ".join(
                    [
                        observation.as_of_date.isoformat(),
                        observation.action,
                        f"{observation.momentum_return:.2%}",
                        f"{observation.forward_return:.2%}",
                        observation.future_price_date.isoformat(),
                    ]
                )
            )

        typer.echo(f"Completed observations | {len(observations)}")
        typer.echo("")

    summaries = summarize_momentum_backtest(observations)

    typer.echo(
        "Action | Observations | Average forward return | "
        "Median forward return | Directional result | Hit rate"
    )

    for summary in summaries:
        if summary.directional_hit_rate is None:
            directional_result = "N/A"
            hit_rate = "N/A"
        else:
            directional_result = (
                f"{summary.directional_correct_count}/"
                f"{summary.directional_observations}"
            )
            hit_rate = f"{summary.directional_hit_rate:.2%}"

        typer.echo(
            " | ".join(
                [
                    summary.action,
                    str(summary.observation_count),
                    f"{summary.average_forward_return:.2%}",
                    f"{summary.median_forward_return:.2%}",
                    directional_result,
                    hit_rate,
                ]
            )
        )


@app.command()
def query(
    symbol: Annotated[str | None, typer.Option()] = None,
) -> None:
    """Query canonical daily-price data from DuckDB."""
    settings = get_settings()
    connection = connect(settings.database_path)

    try:
        register_daily_prices_view(
            connection,
            settings.data_dir / "daily_prices",
        )

        sql = """
            SELECT symbol, trade_date, close_price
            from daily_prices
        """

        parameters: list[str] = []

        if symbol:
            sql += " WHERE symbol = ?"
            parameters.append(symbol.upper())

        sql += " ORDER BY symbol, trade_date"

        for row in connection.execute(sql, parameters).fetchall():
            typer.echo(" | ".join(str(value) for value in row))

    except duckdb.IOException as error:
        raise typer.BadParameter(
            "No daily-price Parquet data exists yet. Run the load-csv command first"
        ) from error

    finally:
        connection.close()


@app.command("rank-momentum")
def rank_momentum(
    as_of: Annotated[str, typer.Option()],
    limit: Annotated[int, typer.Option()] = 10,
    input_dataset_version: Annotated[str, typer.Option()] = "nse-bhavcopy-v1",
    max_price_age_days: Annotated[int, typer.Option()] = 7,
    symbols_file: Annotated[
        Path | None,
        typer.Option("--symbols-file", exists=True, dir_okay=False),
    ] = None,
) -> None:
    """Rank eligible NSE symbols by baseline momentum score."""
    if limit <= 0:
        raise typer.BadParameter(
            "limit must be greater than zero.",
            param_hint="--limit",
        )

    try:
        as_of_date = date.fromisoformat(as_of)
    except ValueError as error:
        raise typer.BadParameter(
            "Use ISO format YYYY-MM-DD.",
            param_hint="--as-of",
        ) from error

    try:
        allowed_symbols = resolve_symbol_universe(symbols_file)
    except ValueError as error:
        raise typer.BadParameter(
            str(error),
            param_hint="--symbols-file",
        ) from error

    settings = get_settings()

    try:
        candidates = rank_momentum_universe(
            exchange="NSE",
            as_of_date=as_of_date,
            input_dataset_version=input_dataset_version,
            database_path=settings.database_path,
            daily_prices_directory=settings.data_dir / "daily_prices",
            max_price_age_days=max_price_age_days,
            allowed_symbols=allowed_symbols,
        )
    except duckdb.IOException as error:
        raise typer.BadParameter(
            "No canonical daily-price Parquet data exists yet."
        ) from error
    except ValueError as error:
        raise typer.BadParameter(str(error)) from error

    if not candidates:
        typer.echo("No eligible momentum candidates.")
        return

    highest_candidates = candidates[:limit]
    lowest_start_index = max(
        limit,
        len(candidates) - limit,
    )
    lowest_candidates = list(reversed(candidates[lowest_start_index:]))

    typer.echo("Highest momentum scores")
    typer.echo("Rank | Symbol | Price date | Momentum | Score | Action")

    for candidate in highest_candidates:
        typer.echo(
            " | ".join(
                [
                    str(candidate.rank),
                    candidate.symbol,
                    candidate.price_date.isoformat(),
                    f"{candidate.momentum_return:.2%}",
                    f"{candidate.score:.2%}",
                    candidate.action,
                ]
            )
        )

    if lowest_candidates:
        typer.echo("")
        typer.echo("Lowest momentum scores")
        typer.echo("Rank | Symbol | Price date | Momentum | Score | Action")

        for candidate in reversed(candidates[-limit:]):
            typer.echo(
                " | ".join(
                    [
                        str(candidate.rank),
                        candidate.symbol,
                        candidate.price_date.isoformat(),
                        f"{candidate.momentum_return:.2%}",
                        f"{candidate.score:.2%}",
                        candidate.action,
                    ]
                )
            )


@app.command("rank-momentum-record")
def rank_momentum_record(
    as_of: Annotated[str, typer.Option()],
    run_id: Annotated[str, typer.Option()],
    input_dataset_version: Annotated[str, typer.Option()] = "nse-bhavcopy-v1",
    max_price_age_days: Annotated[int, typer.Option()] = 7,
    symbols_file: Annotated[
        Path | None,
        typer.Option("--symbols-file", exists=True, dir_okay=False),
    ] = None,
) -> None:
    """Generate and immutably record one baseline momentum ranking run."""
    try:
        as_of_date = date.fromisoformat(as_of)
    except ValueError as error:
        raise typer.BadParameter(
            "Use ISO format YYYY-MM-DD.",
            param_hint="--as-of",
        ) from error

    try:
        allowed_symbols = resolve_symbol_universe(symbols_file)
    except ValueError as error:
        raise typer.BadParameter(
            str(error),
            param_hint="--symbols-file",
        ) from error

    ranking_run_id = f"baseline_momentum:v1:NSE:{as_of_date.isoformat()}:{run_id}"
    settings = get_settings()

    try:
        result = generate_and_record_momentum_ranking_snapshot(
            ranking_run_id=ranking_run_id,
            exchange="NSE",
            as_of_date=as_of_date,
            generated_at=datetime.now(UTC),
            input_dataset_version=input_dataset_version,
            database_path=settings.database_path,
            daily_prices_directory=settings.data_dir / "daily_prices",
            output_directory=settings.data_dir / "ranking_snapshots",
            max_price_age_days=max_price_age_days,
            allowed_symbols=allowed_symbols,
        )
    except duckdb.IOException as error:
        raise typer.BadParameter(
            "No canonical daily-price Parquet data exists yet."
        ) from error
    except ValueError as error:
        raise typer.BadParameter(str(error)) from error

    typer.echo(f"Ranking run ID | {result.ranking_run.ranking_run_id}")
    typer.echo(f"Eligible candidates | {result.ranking_run.eligible_candidate_count}")
    typer.echo(f"Universe hash | {result.ranking_run.universe_hash}")
    typer.echo(f"Recorded ranking run | {result.run_path}")
    typer.echo(f"Recorded ranking candidates | {result.candidate_path}")


@app.command("rank-momentum-evaluate")
def rank_momentum_evaluate(
    ranking_run_id: Annotated[str, typer.Argument()],
    as_of: Annotated[str, typer.Option()],
) -> None:
    """Evaluate and record completed outcomes for a ranking snapshot."""
    try:
        evaluation_as_of_date = date.fromisoformat(as_of)
    except ValueError as error:
        raise typer.BadParameter(
            "Use ISO format YYYY-MM-DD.",
            param_hint="--as-of",
        ) from error

    settings = get_settings()

    try:
        result = evaluate_and_record_momentum_ranking_snapshot(
            ranking_run_id=ranking_run_id,
            evaluation_as_of_date=evaluation_as_of_date,
            database_path=settings.database_path,
            ranking_snapshots_directory=settings.data_dir / "ranking_snapshots",
            daily_prices_directory=settings.data_dir / "daily_prices",
            evaluations_directory=settings.data_dir / "ranking_evaluations",
        )
    except duckdb.IOException as error:
        raise typer.BadParameter(
            "Required ranking snapshot or canonical daily-price data does not exist."
        ) from error
    except ValueError as error:
        raise typer.BadParameter(str(error)) from error

    typer.echo(f"Ranking run ID | {ranking_run_id}")
    typer.echo(f"Completed evaluations | {result.completed_count}")
    typer.echo(f"Pending evaluations | {result.pending_count}")

    for evaluation in result.evaluations:
        forward_return = (
            f"{evaluation.forward_return:.2%}"
            if evaluation.forward_return is not None
            else "N/A"
        )
        typer.echo(
            f"{evaluation.symbol} | {evaluation.action} | "
            f"{evaluation.status} | {forward_return}"
        )

    for written_path in result.written_paths:
        typer.echo(f"Recorded evaluation | {written_path}")


@app.command("ranking-evaluation-summary")
def ranking_evaluation_summary(
    ranking_run_id: Annotated[str, typer.Argument()],
) -> None:
    """Summarize completed outcomes for one momentum-ranking run."""
    settings = get_settings()

    try:
        summaries = summarize_recorded_ranking_evaluations(
            ranking_run_id=ranking_run_id,
            database_path=settings.database_path,
            evaluations_directory=settings.data_dir / "ranking_evaluations",
            generated_at=datetime.now(UTC),
        )
    except duckdb.IOException as error:
        raise typer.BadParameter(
            "Unable to read recorded ranking evaluations."
        ) from error
    except ValueError as error:
        raise typer.BadParameter(str(error)) from error

    if not summaries:
        typer.echo("No completed ranking evaluations.")
        return

    typer.echo(f"Ranking run ID | {ranking_run_id}")
    typer.echo(
        "Action | Completed | Average forward return | "
        "Median forward return | Directional result | Hit rate"
    )

    for summary in summaries:
        directional_result = (
            f"{summary.directional_correct_count}/{summary.completed_count}"
            if summary.directional_correct_count is not None
            else "N/A"
        )
        hit_rate = (
            f"{summary.directional_hit_rate:.2%}"
            if summary.directional_hit_rate is not None
            else "N/A"
        )

        typer.echo(
            f"{summary.action} | {summary.completed_count} | "
            f"{summary.average_forward_return:.2%} | "
            f"{summary.median_forward_return:.2%} | "
            f"{directional_result} | {hit_rate}"
        )


@app.command("ranking-bucket-summary")
def ranking_bucket_summary(
    ranking_run_id: Annotated[str, typer.Argument()],
    bucket_size: Annotated[int, typer.Option()] = 10,
) -> None:
    """Compare completed top and bottom buckets for a ranking run."""
    settings = get_settings()

    try:
        result = summarize_recorded_ranking_buckets(
            ranking_run_id=ranking_run_id,
            database_path=settings.database_path,
            evaluations_directory=settings.data_dir / "ranking_evaluations",
            bucket_size=bucket_size,
            generated_at=datetime.now(UTC),
        )
    except duckdb.IOException as error:
        raise typer.BadParameter(
            "Unable to read recorded ranking evaluations."
        ) from error
    except ValueError as error:
        raise typer.BadParameter(str(error)) from error

    if not result.summaries:
        typer.echo("No completed ranking evaluations.")
        return

    typer.echo(f"Ranking run ID | {ranking_run_id}")
    typer.echo(f"Bucket size | {bucket_size}")
    typer.echo(
        "Bucket | Rank range | Completed | Average forward return | "
        "Median forward return | Positive result | Positive rate"
    )

    for summary in result.summaries:
        positive_result = f"{summary.positive_return_count}/{summary.completed_count}"

        typer.echo(
            f"{summary.bucket} | "
            f"{summary.rank_start}-{summary.rank_end} | "
            f"{summary.completed_count} | "
            f"{summary.average_forward_return:.2%} | "
            f"{summary.median_forward_return:.2%} | "
            f"{positive_result} | "
            f"{summary.positive_return_rate:.2%}"
        )

    typer.echo(f"Average rank spread | {result.average_forward_return_spread:.2%}")


@app.command("backtest-ranking-momentum")
def backtest_ranking_momentum(
    start: Annotated[str, typer.Option()],
    end: Annotated[str, typer.Option()],
    evaluation_as_of: Annotated[str, typer.Option()],
    symbols_file: Annotated[
        Path,
        typer.Option(exists=True, dir_okay=False),
    ],
    bucket_size: Annotated[int, typer.Option()] = 10,
    step_observations: Annotated[int, typer.Option()] = 20,
    input_dataset_version: Annotated[str, typer.Option()] = ("nse-bhavcopy-v1"),
) -> None:
    """Backtest top-versus-bottom momentum ranking performance."""
    try:
        start_date = date.fromisoformat(start)
        end_date = date.fromisoformat(end)
        evaluation_as_of_date = date.fromisoformat(evaluation_as_of)
    except ValueError as error:
        raise typer.BadParameter(
            "Use ISO format YYYY-MM-DD for all date options."
        ) from error

    allowed_symbols = resolve_symbol_universe(symbols_file)
    if allowed_symbols is None:
        raise typer.BadParameter("A symbols file is required.")

    settings = get_settings()

    try:
        result = run_momentum_ranking_backtest(
            exchange="NSE",
            start_date=start_date,
            end_date=end_date,
            evaluation_as_of_date=evaluation_as_of_date,
            allowed_symbols=allowed_symbols,
            bucket_size=bucket_size,
            step_observations=step_observations,
            input_dataset_version=input_dataset_version,
            database_path=settings.database_path,
            daily_prices_directory=settings.data_dir / "daily_prices",
            generated_at=datetime.now(UTC),
        )
    except duckdb.IOException as error:
        raise typer.BadParameter(
            "No canonical daily-price Parquet data exists yet."
        ) from error
    except ValueError as error:
        raise typer.BadParameter(str(error)) from error

    if result.summary is None:
        typer.echo("No completed ranking dates.")
        return

    typer.echo(
        "Ranking date | Top average return | Bottom average return | Rank spread"
    )
    for observation in result.observations:
        typer.echo(
            f"{observation.ranking_date.isoformat()} | "
            f"{observation.top_average_forward_return:.2%} | "
            f"{observation.bottom_average_forward_return:.2%} | "
            f"{observation.rank_spread:.2%}"
        )

    summary = result.summary
    typer.echo(f"Ranking dates attempted | {summary.attempted_ranking_date_count}")
    typer.echo(f"Ranking dates completed | {summary.completed_ranking_date_count}")
    typer.echo(f"Positive rank spreads | {summary.positive_rank_spread_count}")
    typer.echo(f"Rank-spread hit rate | {summary.rank_spread_hit_rate:.2%}")
    typer.echo(f"Average rank spread | {summary.average_rank_spread:.2%}")
    typer.echo(f"Median rank spread | {summary.median_rank_spread:.2%}")


@app.command("pipeline-run-status")
def pipeline_run_status(
    run_id: Annotated[str, typer.Argument()],
) -> None:
    """Display the latest stored state for one pipeline run."""
    settings = get_settings()

    try:
        pipeline_run = get_pipeline_run_status(
            run_id=run_id,
            database_path=settings.database_path,
            pipeline_runs_directory=settings.data_dir / "pipeline_runs",
        )
    except duckdb.IOException as error:
        raise typer.BadParameter("Unable to read pipeline-run records.") from error
    except ValueError as error:
        raise typer.BadParameter(str(error)) from error

    if pipeline_run is None:
        typer.echo("Pipeline run not found.")
        return

    finished_at = (
        pipeline_run.finished_at.isoformat()
        if pipeline_run.finished_at is not None
        else "N/A"
    )

    typer.echo(f"Run ID | {pipeline_run.run_id}")
    typer.echo(f"Pipeline | {pipeline_run.pipeline_name}")
    typer.echo(f"Status | {pipeline_run.status}")
    typer.echo(f"Started | {pipeline_run.started_at.isoformat()}")
    typer.echo(f"Finished | {finished_at}")
    typer.echo(
        f"Files | {pipeline_run.files_processed}/"
        f"{pipeline_run.files_discovered} processed"
    )
    typer.echo(
        f"Rows | {pipeline_run.valid_rows} valid | "
        f"{pipeline_run.rejected_rows} rejected"
    )

    if pipeline_run.error_message is not None:
        typer.echo(f"Error | {pipeline_run.error_message}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
    )
    app()
