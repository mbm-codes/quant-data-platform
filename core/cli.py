from pathlib import Path
from typing import Annotated

import duckdb
import typer

from core.config.settings import get_settings
from core.data.daily_prices import load_daily_prices_csv
from core.data.duckdb import connect, register_daily_prices_view
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