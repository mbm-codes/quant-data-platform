from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import duckdb
import typer

from core.config.settings import get_settings
from core.data.daily_prices import load_daily_prices_csv
from core.data.duckdb import connect, register_daily_prices_view
from core.data.kaggle_inventory import (
    inventory_kaggle_historical_directory,
    write_historical_source_inventory,
)
from core.data.kaggle_staging import stage_kaggle_historical_directory
from core.data.parquet import write_daily_prices_parquet

app = typer.Typer(no_args_is_help=True)

@app.command("load-csv")
def load_csv(
    source: Annotated[Path, typer.Argument(exists=True, dir_okay=False)],
) -> None:
    """Validate a CSV file and write it as a canonical parquet."""

    settings = get_settings()
    records = load_daily_prices_csv(source)

    written_paths = write_daily_prices_parquet(
        records,
        settings.data_dir / "daily_prices",
    )

    for written_path in written_paths:
        typer.echo(f"Wrote {written_path}")

@app.command()
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

    inventory = inventory_kaggle_historical_directory(
        source_directory,
        run_id=run_id,
        dataset_version=dataset_version,
        ingested_at=datetime.now(UTC),
    )

    write_historical_source_inventory(inventory, output_path)
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
        settings.data_dir
        / "staging"
        / "kaggle_historical_prices"
        / f"run_id={run_id}",
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
            "No daily-price Parquet data exists yet. "
            "Run the load-csv command first"
        ) from error
    
    finally:
        connection.close()

if __name__ == "__main__":
    app()