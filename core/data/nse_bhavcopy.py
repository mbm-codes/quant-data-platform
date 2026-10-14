import csv
import gzip
import hashlib
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import TextIO

from core.models.daily_prices import DailyPrice

_MONTH_NUMBERS = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "May": 5,
    "Jun": 6,
    "Jul": 7,
    "Aug": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dec": 12,
}


def load_nse_bhavcopy_file(
    path: Path,
    *,
    run_id: str,
    dataset_version: str,
    ingested_at: datetime,
) -> list[DailyPrice]:
    """Load regularly-equity rows from one official NSE bhavcopy file."""
    source_hash = hashlib.sha256(path.read_bytes()).hexdigest()
    records: list[DailyPrice] = []

    with _open_source_file(path) as source_file:
        reader = csv.DictReader(source_file, skipinitialspace=True)
        for line_number, row in enumerate(reader, start=2):
            if row["SERIES"] != "EQ":
                continue

            try:
                records.append(
                    DailyPrice.model_validate(
                        {
                            "exchange": "NSE",
                            "symbol": row["SYMBOL"].upper(),
                            "trade_date": _parse_trade_date(row["DATE1"]),
                            "open_price": Decimal(row["OPEN_PRICE"]),
                            "high_price": Decimal(row["HIGH_PRICE"]),
                            "low_price": Decimal(row["LOW_PRICE"]),
                            "close_price": Decimal(row["CLOSE_PRICE"]),
                            "volume_qty": int(row["TTL_TRD_QNTY"]),
                            "source_name": "nse_bhavcopy",
                            "source_file": path.name,
                            "source_hash": source_hash,
                            "ingested_at": ingested_at,
                            "run_id": run_id,
                            "dataset_version": dataset_version,
                        }
                    )
                )
            except (KeyError, ValueError) as error:
                raise ValueError(
                    f"Invalid NSE bhavcopy row in {path.name}"
                    f"at line {line_number}: {error}"
                ) from error

    return records


def _parse_trade_date(value: str) -> date:
    try:
        day, month, year = value.split("-")
        return date(int(year), _MONTH_NUMBERS[month], int(day))
    except (KeyError, ValueError) as error:
        raise ValueError(f"Invalid DATE1 value: {value!r}") from error


def _open_source_file(path: Path) -> TextIO:
    if path.suffix == ".gz":
        return gzip.open(path, mode="rt", encoding="utf-8", newline="")

    return path.open(mode="r", encoding="utf-8", newline="")
