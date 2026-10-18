from pathlib import Path

import duckdb


def connect(database_path: Path) -> duckdb.DuckDBPyConnection:
    """Open a local DuckDB database, creating its parent directory if needed."""

    database_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(database_path))


def register_daily_prices_view(
    connection: duckdb.DuckDBPyConnection,
    parquet_directory: Path,
) -> None:
    """Expose year-partitioned daily-price parquet files as a DuckDB view."""

    parquet_glob = (
        (parquet_directory / "trade_year=*" / "*.parquet").as_posix().replace("'", "''")
    )

    connection.execute(
        f"""
        CREATE OR REPLACE VIEW daily_prices AS
        SELECT *
        FROM read_parquet(
            '{parquet_glob}',
            hive_partitioning = true
        );
        """
    )


def register_recommendations_view(
    connection: duckdb.DuckDBPyConnection,
    parquet_directory: Path,
) -> None:
    """Expose recommendation-ledger Parquet files as a DuckDB view."""
    parquet_glob = (
        (parquet_directory / "as_of_year=*" / "*.parquet").as_posix().replace("'", "'")
    )

    connection.execute(
        f"""
        CREATE OR REPLACE VIEW recommendations AS
        SELECT *
        FROM read_parquet(
            '{parquet_glob}',
            hive_partitioning = true
        );
        """
    )
