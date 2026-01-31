from dataclasses import dataclass
from core.context.process_context import ProcessContext
from core.control.job_control import JobControl


@dataclass
class ExecutionContext:
    process: ProcessContext
    job_control: JobControl
    