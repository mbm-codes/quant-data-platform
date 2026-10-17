import hashlib
from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from core.models.daily_prices import DailyPrice

DAILY_PRICES_SCHEMA = pa.schema(
    [
        pa.field("exchange", pa.string(), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("trade_date", pa.date32(), nullable=False),
        pa.field("open_price", pa.decimal128(20, 8), nullable=False),
        pa.field("high_price", pa.decimal128(20, 8), nullable=False),
        pa.field("low_price", pa.decimal128(20, 8), nullable=False),
        pa.field("close_price", pa.decimal128(20, 8), nullable=False),
        pa.field("volume_qty", pa.int64(), nullable=False),
        pa.field("currency", pa.string(), nullable=False),
        pa.field("price_basis", pa.string(), nullable=False),
        pa.field("source_name", pa.string(), nullable=False),
        pa.field("source_file", pa.string(), nullable=False),
        pa.field("source_hash", pa.string(), nullable=True),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("dataset_version", pa.string(), nullable=False),
    ]
)


def write_daily_prices_parquet(
    records: list[DailyPrice],
    output_directory: Path,
) -> list[Path]:
    """Write validated daily prices into idempotent Parquet files."""
    _validate_unique_business_keys(records)

    records_by_source: dict[tuple[int, str | None], list[DailyPrice]] = defaultdict(
        list
    )

    for record in records:
        source_key = record.source_file if record.source_hash else None
        records_by_source[(record.trade_date.year, source_key)].append(record)

    written_paths: list[Path] = []

    for (trade_year, source_key), source_records in sorted(
        records_by_source.items(),
        key=lambda item: (item[0][0], item[0][1] or ""),
    ):
        partition_directory = output_directory / f"trade_year={trade_year}"
        partition_directory.mkdir(parents=True, exist_ok=True)

        output_path = partition_directory / _output_filename(source_key)
        temporary_path = output_path.with_suffix(".tmp")

        table = pa.Table.from_pylist(
            [record.model_dump(mode="python") for record in source_records],
            schema=DAILY_PRICES_SCHEMA,
        )

        pq.write_table(table, temporary_path)
        temporary_path.replace(output_path)
        written_paths.append(output_path)

    return written_paths


def _output_filename(source_key: str | None) -> str:
    if source_key is None:
        return "daily_prices.parquet"

    source_token = hashlib.sha256(source_key.encode("utf-8")).hexdigest()
    return f"source_file={source_token}.parquet"


def _validate_unique_business_keys(records: list[DailyPrice]) -> None:
    keys = [(record.exchange, record.symbol, record.trade_date) for record in records]

    if len(keys) != len(set(keys)):
        raise ValueError("daily_prices contains duplicate business keys.")
