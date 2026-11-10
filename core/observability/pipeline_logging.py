import json
import logging

from core.models.pipeline_log_event import PipelineLogEvent

LOG_LEVELS = {
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
}


def emit_pipeline_log_event(
    event: PipelineLogEvent,
) -> None:
    """Emit one validated pipeline event as a JSON log message."""
    logger = logging.getLogger("qdp.pipeline")

    logger.log(
        LOG_LEVELS[event.level],
        json.dumps(
            event.model_dump(mode="json"),
            sort_keys=True,
        ),
    )
