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


def register_recommendation_evaluations_view(
    connection: duckdb.DuckDBPyConnection,
    parquet_directory: Path,
) -> None:
    """Expose recommendation-evaluation Parquet files as a DuckDB view."""
    parquet_glob = (
        (parquet_directory / "evaluation_year=*" / "*.parquet")
        .as_posix()
        .replace("'", "''")
    )

    connection.execute(
        f"""
        CREATE OR REPLACE VIEW recommendation_evaluations AS
        SELECT *
        FROM read_parquet(
            '{parquet_glob}',
            hive_partitioning = true
        );
        """
    )


def register_momentum_ranking_snapshot_views(
    connection: duckdb.DuckDBPyConnection,
    snapshot_directory: Path,
) -> None:
    """Expose momentum ranking snapshot Parquet files as DuckDB views."""
    runs_glob = (
        (snapshot_directory / "runs" / "as_of_year=*" / "*.parquet")
        .as_posix()
        .replace("'", "''")
    )
    candidates_glob = (
        (snapshot_directory / "candidates" / "as_of_year=*" / "*.parquet")
        .as_posix()
        .replace("'", "''")
    )

    connection.execute(
        f"""
        CREATE OR REPLACE VIEW momentum_ranking_runs AS
        SELECT *
        FROM read_parquet(
            '{runs_glob}',
            hive_partitioning = true
        );
        """
    )
    connection.execute(
        f"""
        CREATE OR REPLACE VIEW momentum_ranking_candidates AS
        SELECT *
        FROM read_parquet(
            '{candidates_glob}',
            hive_partitioning = true
        );
        """
    )


def register_ranking_candidate_evaluations_view(
    connection: duckdb.DuckDBPyConnection,
    evaluations_directory: Path,
) -> None:
    """Expose completed ranking-candidate evaluations as a DuckDB view."""

    parquet_glob = (
        (evaluations_directory / "evaluation_year=*" / "*.parquet")
        .as_posix()
        .replace("'", "''")
    )

    connection.execute(
        f"""
        CREATE OR REPLACE VIEW ranking_candidate_evaluations AS
        SELECT *
        FROM read_parquet(
            '{parquet_glob}',
            hive_partitioning = true
        );
        """
    )

def register_pipeline_runs_view(
    connection: duckdb.DuckDBPyConnection,
    pipeline_runs_directory: Path,
) -> None:
    """Expose pipeline-run state records as a DuckDB view."""
    parquet_glob = (
        pipeline_runs_directory / "started_year=*" / "*.parquet"
    ).as_posix().replace("'", "''")

    connection.execute(
        f"""
        CREATE OR REPLACE VIEW pipeline_runs AS
        SELECT *
        FROM read_parquet(
            '{parquet_glob}',
            hive_partitioning = true
        );
        """
    )