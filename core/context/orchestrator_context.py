from dataclasses import dataclass


@dataclass(frozen=True)
class OrchestratorContext:
    run_id: str | None = None
    process_name: str | None = None
    is_backfill: bool | None = None
    execution_date: str | None = None


def normalize_orchestrator_context(
        ctx: dict | OrchestratorContext | None
) -> dict:
    
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