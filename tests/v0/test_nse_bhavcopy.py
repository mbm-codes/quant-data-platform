import gzip
from datetime import UTC, datetime
from pathlib import Path

from core.data.nse_bhavcopy import load_nse_bhavcopy_file


def test_loads_only_eq_rows_from_nse_bhavcopy(tmp_path: Path) -> None:
    source_path = tmp_path / "bhavcopy_2024-01-01.csv.gz"

    with gzip.open(source_path, mode="wt", encoding="utf-8", newline="") as source_file:
        source_file.write(
            "SYMBOL,SERIES,DATE1,PREV_CLOSE,OPEN_PRICE,HIGH_PRICE,"
            "LOW_PRICE,LAST_PRICE,CLOSE_PRICE,AVG_PRICE,TTL_TRD_QNTY,"
            "TURNOVER_LACS,NO_OF_TRADES,DELIV_QTY,DELIV_PER\n"
            "RELIANCE,EQ,01-Jan-2024,2500.00,2510.00,2530.00,2505.00,"
            "2520.00,2520.00,2518.00,1000,25.18,50,600,60.00\n"
            "1018GS2026,GS,01-Jan-2024,120.47,114.45,126.49,114.45,"
            "126.49,126.49,125.78,17,0.02,4,16,94.12\n"
            "21STCENMGM,BE,01-Jan-2024,32.95,33.30,33.50,32.30,"
            "32.30,32.30,32.63,16413,5.36,90,-,-\n",
        )

    records = load_nse_bhavcopy_file(
        source_path,
        run_id="test-run",
        dataset_version="nse-bhavcopy-v1",
        ingested_at=datetime.now(UTC),
    )

    assert len(records) == 1

    record = records[0]
    assert record.exchange == "NSE"
    assert record.symbol == "RELIANCE"
    assert record.trade_date.isoformat() == "2024-01-01"
    assert str(record.open_price) == "2510.00"
    assert record.volume_qty == 1000
    assert record.currency == "INR"
    assert record.price_basis == "raw"
    assert record.source_name == "nse_bhavcopy"
    assert record.source_file == "bhavcopy_2024-01-01.csv.gz"
    assert record.source_hash is not None
