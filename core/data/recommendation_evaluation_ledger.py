import hashlib
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from core.models.recommendation_evaluation import RecommendationEvaluation

RECOMMENDATION_EVALUATION_SCHEMA = pa.schema(
    [
        pa.field("recommendation_id", pa.string(), nullable=False),
        pa.field("evaluation_as_of_date", pa.date32(), nullable=False),
        pa.field("future_observations_found", pa.int64(), nullable=False),
        pa.field("status", pa.string(), nullable=False),
        pa.field("future_price_date", pa.date32(), nullable=False),
        pa.field("future_close_price", pa.decimal128(20, 8), nullable=False),
        pa.field("forward_return", pa.decimal128(20, 8), nullable=False),
    ]
)


def write_completed_recommendation_evaluations(
    evaluations: list[RecommendationEvaluation],
    output_directory: Path,
) -> list[Path]:
    """Write completed recommendation evaluations to an immutable ledger."""
    recommendation_ids: set[str] = set()
    written_paths: list[Path] = []

    for evaluation in evaluations:
        if evaluation.status != "COMPLETED":
            raise ValueError("Only completed evaluations can be written to the ledger.")

        if evaluation.future_price_date is None:
            raise ValueError("Completed evaluations require a future price date.")
        if evaluation.recommendation_id in recommendation_ids:
            raise ValueError(
                "Duplicate recommendation ID in write request: "
                f"{evaluation.recommendation_id}"
            )

        recommendation_ids.add(evaluation.recommendation_id)

        partition_directory = (
            output_directory / f"evaluation_year={evaluation.future_price_date.year}"
        )
        partition_directory.mkdir(parents=True, exist_ok=True)

        output_path = partition_directory / _evaluation_filename(
            evaluation.recommendation_id
        )
        if output_path.exists():
            raise ValueError(
                "Evaluation already exists in ledger for recommendation: "
                f"{evaluation.recommendation_id}"
            )

        temporary_path = output_path.with_suffix(".tmp")
        table = pa.Table.from_pylist(
            [evaluation.model_dump(mode="python")],
            schema=RECOMMENDATION_EVALUATION_SCHEMA,
        )

        pq.write_table(table, temporary_path)
        temporary_path.replace(output_path)
        written_paths.append(output_path)

    return written_paths


def _evaluation_filename(recommendation_id: str) -> str:
    identifier_hash = hashlib.sha256(recommendation_id.encode("utf-8")).hexdigest()
    return f"recommendation_id={identifier_hash}.parquet"
