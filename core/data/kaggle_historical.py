import csv
import gzip
from dataclasses import dataclass
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path

from core.models.historical_source_prices import IST, HistoricalSourcePrice

REQUIRED_COLUMNS = {
    "Date",
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
    "Dividends",
    "Stock Splits",
}

OPTIONAL_COLUMNS = {"Capital Gains"}
PRICE_QUANTUM = Decimal("0.00000001")


@dataclass(frozen=True)
class HistoricalRowRejection:
    source_file: str
    source_symbol: str
    line_number: int
    raw_row: dict[str, str | None]
    reason: str


@dataclass(frozen=True)
class HistoricalFileLoadResult:
    records: list[HistoricalSourcePrice]
    rejections: list[HistoricalRowRejection]


def normalize_price(value: str) -> Decimal:
    """Normalize source floating-point values to V0 decimal precision."""
    try:
        return Decimal(value).quantize(
            PRICE_QUANTUM,
            rounding=ROUND_HALF_UP,
        )
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"Invalid price value: {value!r}") from error


def load_kaggle_historical_file_with_rejections(
    path: Path,
    *,
    run_id: str,
    dataset_version: str,
    ingested_at: datetime,
) -> HistoricalFileLoadResult:
    """Load one source file, retaining valid rows and quarantining invalid rows."""
    source_symbol = _source_symbol_from_filename(path)
    symbol = source_symbol.removesuffix(".NS")

    with gzip.open(path, mode="rt", encoding="utf-8", newline="") as source_file:
        reader = csv.DictReader(source_file)
        fieldnames = set(reader.fieldnames or [])
        missing_columns = REQUIRED_COLUMNS - fieldnames
        unexpected_columns = fieldnames - REQUIRED_COLUMNS - OPTIONAL_COLUMNS

        if missing_columns or unexpected_columns:
            raise ValueError(
                f"Unexpected columns in {path.name}: {reader.fieldnames}"
            )

        records: list[HistoricalSourcePrice] = []
        rejections: list[HistoricalRowRejection] = []

        for line_number, row in enumerate(reader, start=2):
            try:
                source_timestamp = datetime.fromisoformat(row["Date"])

                records.append(
                    HistoricalSourcePrice.model_validate(
                        {
                            "source_symbol": source_symbol,
                            "symbol": symbol,
                            "source_file": path.name,
                            "source_timestamp": source_timestamp,
                            "trade_date": source_timestamp.astimezone(IST).date(),
                            "open_price": normalize_price(row["Open"]),
                            "high_price": normalize_price(row["High"]),
                            "low_price": normalize_price(row["Low"]),
                            "close_price": normalize_price(row["Close"]),
                            "volume_qty": row["Volume"],
                            "dividends": normalize_price(row["Dividends"]),
                            "stock_splits": normalize_price(row["Stock Splits"]),
                            "ingested_at": ingested_at,
                            "run_id": run_id,
                            "dataset_version": dataset_version,
                        }
                    )
                )
            except ValueError as error:
                rejections.append(
                    HistoricalRowRejection(
                        source_file=path.name,
                        source_symbol=source_symbol,
                        line_number=line_number,
                        raw_row=dict(row),
                        reason=str(error),
                    )
                )

    return HistoricalFileLoadResult(records=records, rejections=rejections)


def load_kaggle_historical_file(
    path: Path,
    *,
    run_id: str,
    dataset_version: str,
    ingested_at: datetime,
) -> list[HistoricalSourcePrice]:
    """Load one source file and fail if it contains rejected rows."""
    result = load_kaggle_historical_file_with_rejections(
        path,
        run_id=run_id,
        dataset_version=dataset_version,
        ingested_at=ingested_at,
    )

    if result.rejections:
        first_rejection = result.rejections[0]
        raise ValueError(
            f"Invalid row in {first_rejection.source_file} at line "
            f"{first_rejection.line_number}: {first_rejection.reason}"
        )

    return result.records


def _source_symbol_from_filename(path: Path) -> str:
    if not path.name.endswith(".NS.csv.gz"):
        raise ValueError(
            f"Expected an NSE source filename ending in .NS.csv.gz: {path.name}"
        )

    return path.name.removesuffix(".csv.gz").upper()