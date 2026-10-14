import hashlib
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from core.data.nse_bhavcopy import load_nse_bhavcopy_file
from core.data.parquet import write_daily_prices_parquet
from core.models.daily_prices import DailyPrice


@dataclass(frozen=True)
class NseBhavcopyStagingRunResult:
    files_discovered: int
    files_processed: int
    files_failed: int
    duplicate_files_skipped: int
    valid_rows: int
    failed_files: list[str]


def stage_nse_bhavcopy_directory(
    source_directory: Path,
    output_directory: Path,
    *,
    run_id: str,
    dataset_version: str,
    ingested_at: datetime,
) -> NseBhavcopyStagingRunResult:
    """Stage official NSE bhavcopy files into canonical daily prices."""
    source_paths = sorted(source_directory.glob("*.csv.gz"))
    files_processed = 0
    duplicate_files_skipped = 0
    valid_rows = 0
    failed_files: list[str] = []
    fingerprints_by_trade_date: dict[date, str] = {}

    for source_path in source_paths:
        try:
            records = load_nse_bhavcopy_file(
                source_path,
                run_id=run_id,
                dataset_version=dataset_version,
                ingested_at=ingested_at,
            )

            if not records:
                files_processed += 1
                continue

            trade_dates = {record.trade_date for record in records}
            if len(trade_dates) != 1:
                raise ValueError(
                    f"Expected one trade date in {source_path.name},"
                    f"Found {sorted(trade_dates)}"
                )

            trade_date = next(iter(trade_dates))
            fingerprint = _canonical_records_fingerprint(records)
            existing_fingerprint = fingerprints_by_trade_date.get(trade_date)

            if existing_fingerprint is not None:
                if existing_fingerprint != fingerprint:
                    raise ValueError(
                        f"Conflicting bhavcopy content for {trade_date}:"
                        f"{source_path.name}"
                    )

                duplicate_files_skipped += 1
                files_processed += 1
                continue

            write_daily_prices_parquet(records, output_directory)
            fingerprints_by_trade_date[trade_date] = fingerprint
        except (OSError, ValueError):
            failed_files.append(source_path.name)
            continue

        files_processed += 1
        valid_rows += len(records)

    return NseBhavcopyStagingRunResult(
        files_discovered=len(source_paths),
        files_processed=files_processed,
        files_failed=len(failed_files),
        duplicate_files_skipped=duplicate_files_skipped,
        valid_rows=valid_rows,
        failed_files=failed_files,
    )


def _canonical_records_fingerprint(records: list[DailyPrice]) -> str:
    rows = [
        "|".join(
            [
                record.exchange,
                record.symbol,
                record.trade_date.isoformat(),
                format(record.open_price, "f"),
                format(record.high_price, "f"),
                format(record.low_price, "f"),
                format(record.close_price, "f"),
                str(record.volume_qty),
                record.currency,
                record.price_basis,
            ]
        )
        for record in sorted(
            records, key=lambda record: (record.exchange, record.symbol)
        )
    ]

    return hashlib.sha256("\n".join(rows).encode("utf-8")).hexdigest()
