import json
import logging
from datetime import UTC, datetime

from core.models.pipeline_log_event import PipelineLogEvent
from core.observability.pipeline_logging import emit_pipeline_log_event


def test_emits_pipeline_event_as_json(caplog) -> None:
    event = PipelineLogEvent(
        timestamp=datetime(2026, 1, 20, 9, tzinfo=UTC),
        level="INFO",
        event="pipeline_completed",
        run_id="stage-bhavcopy-001",
        pipeline_name="stage_bhavcopy",
        status="COMPLETED",
        files_discovered=1,
        files_processed=1,
        valid_rows=1_818,
        rejected_rows=0,
    )

    with caplog.at_level(logging.INFO, logger="qdp.pipeline"):
        emit_pipeline_log_event(event)

    logged_event = json.loads(caplog.records[-1].getMessage())

    assert logged_event["event"] == "pipeline_completed"
    assert logged_event["run_id"] == "stage-bhavcopy-001"
    assert logged_event["status"] == "COMPLETED"
    assert logged_event["valid_rows"] == 1_818
