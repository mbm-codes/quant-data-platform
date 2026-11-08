import hashlib
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from core.models.ranking_evaluation import MomentumRankingCandidateEvaluation

RANKING_EVALUATION_SCHEMA = pa.schema(
    [
        pa.field("ranking_run_id", pa.string(), nullable=False),
        pa.field("exchange", pa.string(), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("rank", pa.int64(), nullable=False),
        pa.field("action", pa.string(), nullable=False),
        pa.field("price_date", pa.date32(), nullable=False),
        pa.field("reference_price", pa.decimal128(20, 8), nullable=False),
        pa.field("input_dataset_version", pa.string(), nullable=False),
        pa.field("evaluation_as_of_date", pa.date32(), nullable=False),
        pa.field("future_observations_found", pa.int64(), nullable=False),
        pa.field("status", pa.string(), nullable=False),
        pa.field("future_price_date", pa.date32(), nullable=False),
        pa.field("future_close_price", pa.decimal128(20, 8), nullable=False),
        pa.field("forward_return", pa.decimal128(20, 8), nullable=False),
    ]
)


def write_completed_ranking_candidate_evaluations(
    evaluations: list[MomentumRankingCandidateEvaluation],
    output_directory: Path,
) -> list[Path]:
    """Write immutable completed ranking-candidate evaluations."""
    evaluation_keys: set[tuple[str, int]] = set()
    written_paths: list[Path] = []

    for evaluation in evaluations:
        if evaluation.status != "COMPLETED":
            raise ValueError("Only completed ranking evaluations can be recorded.")

        evaluation_key = (evaluation.ranking_run_id, evaluation.rank)
        if evaluation_key in evaluation_keys:
            raise ValueError(
                "Duplicate ranking evaluation in write request: "
                f"{evaluation.ranking_run_id}, rank {evaluation.rank}"
            )
        evaluation_keys.add(evaluation_key)

        partition_directory = (
            output_directory
            / f"evaluation_year={evaluation.evaluation_as_of_date.year}"
        )
        partition_directory.mkdir(parents=True, exist_ok=True)

        output_path = partition_directory / _evaluation_filename(
            evaluation.ranking_run_id,
            evaluation.rank,
        )
        if output_path.exists():
            raise ValueError(
                "Ranking evaluation already exists: "
                f"{evaluation.ranking_run_id}, rank {evaluation.rank}"
            )

        table = pa.Table.from_pylist(
            [evaluation.model_dump(mode="python")],
            schema=RANKING_EVALUATION_SCHEMA,
        )

        temporary_path = output_path.with_suffix(".tmp")
        pq.write_table(table, temporary_path)
        temporary_path.replace(output_path)
        written_paths.append(output_path)

    return written_paths


def _evaluation_filename(ranking_run_id: str, rank: int) -> str:
    identifier = f"{ranking_run_id}:{rank}"
    identifier_hash = hashlib.sha256(identifier.encode("utf-8")).hexdigest()
    return f"ranking_candidate_evaluation_id={identifier_hash}.parquet"
