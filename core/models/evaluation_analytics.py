from dataclasses import dataclass
from datetime import date
from decimal import Decimal


@dataclass(frozen=True)
class CompletedEvaluationRow:
    """A completed evaluation with recorded recommendation context."""

    recommendation_id: str
    account_id: str
    strategy_name: str
    strategy_version: str
    action: str
    symbol: str
    as_of_date: date
    future_price_date: date
    forward_return: Decimal


@dataclass(frozen=True)
class EvaluationSummary:
    """Completed-evaluation metrics for one strategy and action."""

    strategy_name: str
    strategy_version: str
    action: str
    completed_count: int
    average_forward_return: Decimal
    directional_observations: int
    directional_correct_count: int
    directional_hit_rate: Decimal | None
