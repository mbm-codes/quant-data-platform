from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

from core.data.pipeline_run_store import write_pipeline_run
from core.models.pipeline_run import PipelineRun


def make_running_run() -> PipelineRun:
    return PipelineRun(
        run_id="stage-bhavcopy-001",
        pipeline_name="stage_bhavcopy",
        input_dataset_version="nse-bhavcopy-v1",
        status="RUNNING",
        started_at=datetime(2026, 1, 20, 9, tzinfo=UTC),
        files_discovered=1,
        files_processed=0,
        valid_rows=0,
        rejected_rows=0,
    )


def test_writes_pipeline_run_state(tmp_path: Path) -> None:
    output_path = write_pipeline_run(
        make_running_run(),
        tmp_path,
    )

    assert output_path.parent == tmp_path / "started_year=2026"
    assert output_path.suffix == ".parquet"

    table = pq.read_table(output_path)
    assert table.num_rows == 1
    assert table.column("status").to_pylist() == ["RUNNING"]


def test_replaces_existing_pipeline_run_state(tmp_path: Path) -> None:
    running_run = make_running_run()
    write_pipeline_run(running_run, tmp_path)

    completed_run = running_run.model_copy(
        update={
            "status": "COMPLETED",
            "finished_at": datetime(2026, 1, 20, 9, 2, tzinfo=UTC),
            "files_processed": 1,
            "valid_rows": 1_818,
        }
    )
    output_path = write_pipeline_run(completed_run, tmp_path)

    written_paths = list(tmp_path.rglob("*.parquet"))

    assert written_paths == [output_path]

    table = pq.read_table(output_path)
    assert table.column("status").to_pylist() == ["COMPLETED"]
    assert table.column("valid_rows").to_pylist() == [1_818]