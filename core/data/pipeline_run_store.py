import hashlib
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from core.models.pipeline_run import PipelineRun

PIPELINE_RUN_SCHEMA = pa.schema(
    [
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("pipeline_name", pa.string(), nullable=False),
        pa.field("input_dataset_version", pa.string(), nullable=False),
        pa.field("status", pa.string(), nullable=False),
        pa.field("started_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("finished_at", pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("files_discovered", pa.int64(), nullable=False),
        pa.field("files_processed", pa.int64(), nullable=False),
        pa.field("valid_rows", pa.int64(), nullable=False),
        pa.field("rejected_rows", pa.int64(), nullable=False),
        pa.field("error_message", pa.string(), nullable=True),
    ]
)


def write_pipeline_run(
    pipeline_run: PipelineRun,
    output_directory: Path,
) -> Path:
    """Write or replace the current state for one pipeline run."""
    partition_directory = (
        output_directory
        / f"started_year={pipeline_run.started_at.year}"
    )
    partition_directory.mkdir(parents=True, exist_ok=True)

    output_path = partition_directory / _pipeline_run_filename(
        pipeline_run.run_id
    )
    temporary_path = output_path.with_suffix(".tmp")

    table = pa.Table.from_pylist(
        [pipeline_run.model_dump(mode="python")],
        schema=PIPELINE_RUN_SCHEMA,
    )

    pq.write_table(table, temporary_path)
    temporary_path.replace(output_path)

    return output_path


def _pipeline_run_filename(run_id: str) -> str:
    run_id_hash = hashlib.sha256(run_id.encode("utf-8")).hexdigest()
    return f"run_id={run_id_hash}.parquet"