import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from core.data.kaggle_historical import (
    HistoricalRowRejection,
    load_kaggle_historical_file_with_rejections,
)


@dataclass(frozen=True)
class HistoricalFileInventory:
    source_file: str
    symbol: str | None
    row_count: int
    first_trade_date: date | None
    last_trade_date: date | None
    rejected_rows: tuple[HistoricalRowRejection, ...]
    error: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "source_file": self.source_file,
            "symbol": self.symbol,
            "row_count": self.row_count,
            "rejected_row_count": len(self.rejected_rows),
            "first_trade_date": (
                self.first_trade_date.isoformat()
                if self.first_trade_date
                else None
            ),
            "last_trade_date": (
                self.last_trade_date.isoformat()
                if self.last_trade_date
                else None
            ),
            "error": self.error,
            "rejections": [
                {
                    "line_number": rejection.line_number,
                    "reason": rejection.reason,
                    "raw_row": rejection.raw_row,
                }
                for rejection in self.rejected_rows
            ],
        }


@dataclass(frozen=True)
class HistoricalSourceInventory:
    source_name: str
    source_directory: str
    run_id: str
    dataset_version: str
    files: list[HistoricalFileInventory]

    def to_dict(self) -> dict[str, object]:
        processed_files = [file for file in self.files if file.error is None]
        dates = [
            profile_date
            for file in processed_files
            for profile_date in (file.first_trade_date, file.last_trade_date)
            if profile_date is not None
        ]

        return {
            "source_name": self.source_name,
            "source_directory": self.source_directory,
            "run_id": self.run_id,
            "dataset_version": self.dataset_version,
            "files_discovered": len(self.files),
            "files_processed": len(processed_files),
            "files_failed": len(self.files) - len(processed_files),
            "files_with_rejected_rows": sum(
                1 for file in processed_files if file.rejected_rows
            ),
            "total_valid_rows": sum(file.row_count for file in processed_files),
            "total_rejected_rows": sum(
                len(file.rejected_rows) for file in processed_files
            ),
            "first_trade_date": min(dates).isoformat() if dates else None,
            "last_trade_date": max(dates).isoformat() if dates else None,
            "files": [file.to_dict() for file in self.files],
        }


def inventory_kaggle_historical_directory(
    source_directory: Path,
    *,
    run_id: str,
    dataset_version: str,
    ingested_at: datetime,
) -> HistoricalSourceInventory:
    """Validate and profile Kaggle historical files one source file at a time."""
    files: list[HistoricalFileInventory] = []

    for path in sorted(source_directory.glob("*.csv.gz")):
        try:
            result = load_kaggle_historical_file_with_rejections(
                path,
                run_id=run_id,
                dataset_version=dataset_version,
                ingested_at=ingested_at,
            )
        except (OSError, ValueError) as error:
            files.append(
                HistoricalFileInventory(
                    source_file=path.name,
                    symbol=_symbol_from_filename(path),
                    row_count=0,
                    first_trade_date=None,
                    last_trade_date=None,
                    rejected_rows=(),
                    error=str(error),
                )
            )
            continue

        records = result.records

        files.append(
            HistoricalFileInventory(
                source_file=path.name,
                symbol=records[0].symbol if records else _symbol_from_filename(path),
                row_count=len(records),
                first_trade_date=(
                    min(record.trade_date for record in records)
                    if records
                    else None
                ),
                last_trade_date=(
                    max(record.trade_date for record in records)
                    if records
                    else None
                ),
                rejected_rows=tuple(result.rejections),
            )
        )

    return HistoricalSourceInventory(
        source_name="kaggle_nse_historical",
        source_directory=str(source_directory),
        run_id=run_id,
        dataset_version=dataset_version,
        files=files,
    )


def write_historical_source_inventory(
    inventory: HistoricalSourceInventory,
    output_path: Path,
) -> None:
    """Write a JSON inventory report."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(inventory.to_dict(), indent=2) + "\n",
        encoding="utf-8",
    )


def _symbol_from_filename(path: Path) -> str | None:
    if not path.name.endswith(".NS.csv.gz"):
        return None

    return path.name.removesuffix(".csv.gz").removesuffix(".NS").upper()