import duckdb


def test_duckdb_is_available() -> None:
    result = duckdb.sql("SELECT 1 as value").fetchone()
    assert result == (1,)