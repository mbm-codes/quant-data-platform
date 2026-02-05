from dataclasses import dataclass
from typing import Optional, Dict
from core.quality.enums import CheckStatus

@dataclass
class CheckResult:
    check_name: str
    status: CheckStatus
    message: str
    metrics: Optional[Dict[str, float]] = None
    details: Optional[Dict[str, object]] = None