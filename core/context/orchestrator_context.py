from dataclasses import dataclass
from typing import Optional, Dict, Union

@dataclass(frozen=True)
class OrchestratorContext:
    run_id: Optional[str] = None
    process_name: Optional[str] = None
    is_backfill: Optional[bool] = None
    execution_date: Optional[str] = None


def normalize_orchestrator_context(
        ctx: Optional[Union[Dict, OrchestratorContext]]
) -> Dict:
    
    """
    Normalizes orchestrator context from different sources
    (Airflow, Prefect, CLI, None) into a dict.
    """
    if ctx is None:
        return {}
    
    if isinstance(ctx, OrchestratorContext):
        return {k: v for k, v in ctx.__dict__.items() if v is not None}
    
    if isinstance(ctx, dict):
        return {k: v for k, v in ctx.items() if v is not None}
    
    raise TypeError(f"Unsupported orchestrator_context type: {type(ctx)}")