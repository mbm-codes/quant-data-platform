from dataclasses import dataclass

from core.quality.enums import CheckStatus, DQAction


@dataclass
class CheckResult:
    check_name: str
    status: CheckStatus
    action: DQAction
    message: str
    metrics: dict[str, float] | None = None
    details: dict[str, object] | None = None
    invalid_predicate: str | None = None