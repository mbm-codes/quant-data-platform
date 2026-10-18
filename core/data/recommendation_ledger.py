import hashlib
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from core.models.recommendations import PortfolioRecommendation

RECOMMENDATION_LEDGER_SCHEMA = pa.schema(
    [
        pa.field("recommendation_id", pa.string(), nullable=False),
        pa.field("account_id", pa.string(), nullable=False),
        pa.field("exchange", pa.string(), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("action", pa.string(), nullable=False),
        pa.field("as_of_date", pa.date32(), nullable=False),
        pa.field("price_date", pa.date32(), nullable=False),
        pa.field("reference_price", pa.decimal128(20, 8), nullable=False),
        pa.field("currency", pa.string(), nullable=False),
        pa.field("score", pa.decimal128(10, 8), nullable=False),
        pa.field("rationale", pa.string(), nullable=False),
        pa.field("strategy_name", pa.string(), nullable=False),
        pa.field("strategy_version", pa.string(), nullable=False),
        pa.field("generated_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("input_dataset_version", pa.string(), nullable=False),
    ]
)


def write_recommendation_ledger(
    recommendations: list[PortfolioRecommendation],
    output_directory: Path,
) -> list[Path]:
    """Write immutable recommendation records to a Parquet ledger."""
    recommendation_ids: set[str] = set()
    written_paths: list[Path] = []

    for recommendation in recommendations:
        if recommendation.recommendation_id in recommendation_ids:
            raise ValueError(
                "Duplicate recommendation ID in write request: "
                f"{recommendation.recommendation_id}"
            )

        recommendation_ids.add(recommendation.recommendation_id)
        partition_directory = (
            output_directory / f"as_of_year={recommendation.as_of_date.year}"
        )
        partition_directory.mkdir(parents=True, exist_ok=True)

        output_path = partition_directory / _recommendation_filename(
            recommendation.recommendation_id
        )
        if output_path.exists():
            raise ValueError(
                "Recommendation already exists in ledger: "
                f"{recommendation.recommendation_id}"
            )

        temporary_path = output_path.with_suffix(".tmp")
        table = pa.Table.from_pylist(
            [recommendation.model_dump(mode="python")],
            schema=RECOMMENDATION_LEDGER_SCHEMA,
        )

        pq.write_table(table, temporary_path)
        temporary_path.replace(output_path)
        written_paths.append(output_path)

    return written_paths


def _recommendation_filename(recommendation_id: str) -> str:
    identifier_hash = hashlib.sha256(recommendation_id.encode("utf-8")).hexdigest()
    return f"recommendation_id={identifier_hash}.parquet"
