from dataclasses import dataclass
from typing import Optional, Dict
from core.quality.enums import CheckStatus, DQAction

@dataclass
class CheckResult:
    check_name: str
    status: CheckStatus
    action: DQAction
    message: str
    metrics: Optional[Dict[str, float]] = None
    details: Optional[Dict[str, object]] = None
    invalid_predicate: Optional[str] = None