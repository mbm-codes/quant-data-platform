# core/cli.py

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Annotated

import duckdb
import typer

from core.config.settings import get_settings
from core.data.duckdb import connect, register_daily_prices_view
from core.services.bhavcopy_staging import stage_bhavcopy as stage_bhavcopy_service
from core.services.csv_loader import load_csv as load_csv_service
from core.services.historical_inventory import (
    inventory_historical as inventory_historical_service,
)
from core.services.historical_staging import stage_kaggle_historical_directory
from core.services.portfolio import get_portfolio_positions, value_portfolio

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
    result = stage_bhavcopy_service(
        source_directory,
        settings.data_dir / "daily_prices",
        run_id=run_id,
        dataset_version=dataset_version,
        ingested_at=datetime.now(UTC),
    )

    typer.echo(f"Files discovered: {result.files_discovered}")
    typer.echo(f"Files processed: {result.files_processed}")
    typer.echo(f"Files failed: {result.files_failed}")
    typer.echo(f"Duplicate files skipped: {result.duplicate_files_skipped}")
    typer.echo(f"Valid rows staged: {result.valid_rows}")


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


# @app.command("recommendation-preview")
# def recommendation_preview(
#     symbol: Annotated[str, typer.Argument(exists=True)],
#     account_id: Annotated[str, typer.Argument(exists=True)],
#     as_of: Annotated[str, typer.Option()],
# ) -> None:
#     """Provide a preview of recommendation"""

#     try:
#         as_of_date = date.fromisoformat(as_of)
#     except ValueError as error:
#         raise typer.BadParameter(
#             "Use ISO format YYYY-MM-DD.",
#             param_hint="--as-of",
#         ) from error

#     settings = get_settings()
#     transactions = load_portfolio_transactions_csv(
#         source,
#         run_id=run_id,
#         ingested_at=datetime.now(UTC),
#     )
#     positions = calculate_positions(transactions)

#     if not positions:
#         typer.echo("No active positions.")
#         return

#     connection = connect(settings.database_path)

#     try:
#         register_daily_prices_view(
#             connection,
#             settings.data_dir / "daily_prices",
#         )
#         valuations = value_positions(
#             connection,
#             positions,
#             as_of=as_of_date,
#             max_price_age_days=max_price_age_days,
#         )
#     except duckdb.IOException as error:
#         raise typer.BadParameter(
#             "No canonical daily-price Parquet data exists yet. "
#             "Run stage-bhavcopy first."
#         ) from error
#     except ValueError as error:
#         raise typer.BadParameter(str(error)) from error
#     finally:
#         connection.close()

#     for valuation in valuations:
#         typer.echo(
#             " | ".join(
#                 [
#                     valuation.account_id,
#                     valuation.exchange,
#                     valuation.symbol,
#                     str(valuation.quantity),
#                     valuation.price_date.isoformat(),
#                     str(valuation.close_price),
#                     str(valuation.market_value),
#                 ]
#             )
#         )

#     total_market_value = sum(
#         (valuation.market_value for valuation in valuations),
#         start=Decimal(0),
#     )
#     typer.echo(f"Total market value | {total_market_value}")


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


if __name__ == "__main__":
    app()
