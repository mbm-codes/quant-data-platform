from pathlib import Path

import pyarrow.parquet as pq

from core.services.csv_loader import load_csv as load_csv_service


def test_load_csv_writes_canonical_parquet(
    tmp_path: Path,
) -> None:
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

    daily_prices_directory = tmp_path / "daily_prices"

    written_paths = load_csv_service(
        source=source,
        daily_prices_directory=daily_prices_directory,
    )

    expected_path = daily_prices_directory / "trade_year=2026" / "daily_prices.parquet"

    assert written_paths == [expected_path]

    table = pq.read_table(expected_path)

    assert table.num_rows == 1
    assert table.column("symbol").to_pylist() == ["RELIANCE"]
    assert str(table.column("close_price").to_pylist()[0]) == "1415.00000000"
