from decimal import Decimal

from core.quality.base import CheckResult, DataQualityCheck
from core.quality.enums import CheckStatus, DQAction, DQSeverity
from core.quality.policy import decide_action


class UniquenessCheck(DataQualityCheck):
    def __init__(
            self, 
            df, 
            key_columns: list[str], 
            severity: DQSeverity, 
            mostly: Decimal = Decimal("0.8"),
            action_on_warn: DQAction | None = DQAction.NONE, 
            action_on_fail: DQAction | None = DQAction.NONE
            
            ):
        self.df = df
        self.key_columns = key_columns
        self.severity = severity
        self.action_on_warn = action_on_warn
        self.action_on_fail = action_on_fail
        self.mostly = mostly

    def run(self, context=None) -> CheckResult:
        total = self.df.count()
        distinct = self.df.select(*self.key_columns).distinct().count()

        if total == distinct:
            return CheckResult(
                check_name="UniquenessCheck",
                status=CheckStatus.PASS,
                action=DQAction.OBSERVE,
                message="Primary key uniqueness holds",
                 metrics = {
                    "total_rows": total,
                    "distinct_keys": distinct
                }
            )
        
        duplicate_ratio = distinct / total
        status = CheckStatus.WARN if duplicate_ratio >= self.mostly else CheckStatus.FAIL
        dq_action = decide_action(status, self.severity, self.action_on_warn, self.action_on_fail)
        
        cols_sql = ", ".join(f"`{c}`" for c in self.key_columns)
        inv_predicate = cols_sql
        return CheckResult(
                check_name="UniquenessCheck",
                status=status,
                action=dq_action,
                message=f"{total - distinct} duplicate primary keys detected",
                metrics = {
                    "total_rows": total,
                    "distinct_keys": distinct
                },
                invalid_predicate=inv_predicate
            )