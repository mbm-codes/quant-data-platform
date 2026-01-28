from dataclasses import dataclass
from datetime import datetime, timezone
import uuid
import os
from typing import Optional, Dict


# =========================
# Dataclass
# =========================
@dataclass(frozen=True)
class ProcessContext:
    # Identity
    process_id: str
    run_id: str
    process_name: str
    pipeline_version: str
    is_backfill: bool

    # Time (single source of truth)
    process_timestamp: datetime

    @property
    def process_date(self):
        return self.process_timestamp.date()
    
    @property
    def process_time(self):
        return self.process_timestamp.time()


# =========================
# Helpers
# =========================
def _generate_process_id(spark=None, process_name: str = "") -> str:
    """
    Returns a stable process_id.
    Priority:
    1. Spark driver JVM PID + applicationId (if Spark is provided)
    2. Otherwise, generate a Python fallback:
       <process_name>-<timestamp>-<uuid>
    """
    if spark:
        try:
            jvm = spark.sparkContext._jvm
            jvm_pid = (
                jvm.java.lang.management.ManagementFactory
                .getRuntimeMXBean()
                .getName()
                .split("@")[0]
            )
            return f"{spark.sparkContext.applicationId}-{jvm_pid}"
        except Exception:
            pass

    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    name_part = process_name.replace(" ", "_")[:20] if process_name else "python"
    return f"{name_part}-{ts}-{uuid.uuid4().hex[:8]}"


def _generate_run_id(force_new: bool = False, existing_run_id: Optional[str] = None) -> str:
    """
    Generates a run_id for this attempt.
    - force_new=True → always generate a new UUID
    - existing_run_id provided → used if not forcing new
    - else tries RUN_ID env var, else generates UUID
    """
    if force_new or not existing_run_id:
        return str(uuid.uuid4())
    return existing_run_id


def _extract_orchestrator_metadata(orchestrator_context: Optional[Dict] = None) -> Dict:
    """
    Extracts common fields from orchestration tool context.
    Supports Airflow, Prefect, Argo, etc.
    
    Expected keys (if available):
      - run_id
      - process_name / dag_id / flow_name
      - is_backfill / backfill
    """
    if not orchestrator_context:
        return {}

    metadata = {}
    # Logical run ID
    if "run_id" in orchestrator_context:
        metadata["run_id"] = orchestrator_context["run_id"]
    # Process / DAG / flow name
    if "process_name" in orchestrator_context:
        metadata["process_name"] = orchestrator_context["process_name"]
    elif "dag_id" in orchestrator_context:
        metadata["process_name"] = orchestrator_context["dag_id"]
    elif "flow_name" in orchestrator_context:
        metadata["process_name"] = orchestrator_context["flow_name"]
    # Backfill flag
    if "is_backfill" in orchestrator_context:
        metadata["is_backfill"] = orchestrator_context["is_backfill"]
    elif "backfill" in orchestrator_context:
        metadata["is_backfill"] = orchestrator_context["backfill"]

    return metadata


# =========================
# Factory Function
# =========================
def create_process_context(
        pipeline_version: str,
        is_backfill: bool = False,
        process_id: Optional[str] = None,
        run_id: Optional[str] = None,
        process_name: Optional[str] = None,
        spark=None,
        force_new_run_id: bool = False,
        orchestrator_context: Optional[Dict] = None
) -> ProcessContext:
    """
    Creates a ProcessContext with orchestration-awareness, Spark support, and retry-friendly run_id.

    Parameters:
    - pipeline_version: str, code/CI version of the pipeline
    - is_backfill: bool, default False (overridden by orchestrator if provided)
    - process_id: optional physical process ID
    - run_id: optional logical run ID
    - process_name: optional process/job name
    - spark: optional SparkSession
    - force_new_run_id: bool, generate new run_id for retry
    - orchestrator_context: optional dict with orchestration metadata (Airflow, Prefect, etc.)
    """
    ts = datetime.now(timezone.utc)

    # Extract metadata from orchestration tool
    orchestration_metadata = _extract_orchestrator_metadata(orchestrator_context)

    resolved_process_name = process_name or orchestration_metadata.get("process_name") or "python_pipeline"
    resolved_is_backfill = orchestration_metadata.get("is_backfill", is_backfill)
    resolved_run_id = _generate_run_id(
        force_new=force_new_run_id,
        existing_run_id=run_id or orchestration_metadata.get("run_id") or os.getenv("RUN_ID")
    )
    resolved_process_id = process_id or _generate_process_id(spark, resolved_process_name)
    resolved_pipeline_version = os.getenv("PIPELINE_VERSION") or pipeline_version

    return ProcessContext(
        process_id=resolved_process_id,
        run_id=resolved_run_id,
        process_name=resolved_process_name,
        pipeline_version=resolved_pipeline_version,
        process_timestamp=ts,
        is_backfill=bool(resolved_is_backfill)
    )

def process_context_to_string(process_context: ProcessContext) -> str:
   """
   Converts a ProcessContext object to a human-readable string for logging purposes.
   """
   return (f"ProcessID: {process_context.process_id}, "
           f"RunID: {process_context.run_id}, "
           f"ProcessName: {process_context.process_name}, "
           f"PipelineVersion: {process_context.pipeline_version}, "
           f"IsBackfill: {process_context.is_backfill}, "
           f"ProcessTimestamp: {process_context.process_timestamp}")