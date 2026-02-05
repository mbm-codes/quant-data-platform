from core.quality.base import DataQualityCheck, CheckResult
from core.quality.enums import CheckStatus, DQSeverity

class UniquenessCheck(DataQualityCheck):
    def __init__(self, df, key_columns: list[str], severity: DQSeverity):
        self.df = df
        self.key_columns = key_columns
        self.severity = severity

    def run(self, context=None) -> CheckResult:
        total = self.df.count()
        distinct = self.df.select(self.key_columns).distinct().count()

        if total == distinct:
            return CheckResult(
                check_name="UniquenessCheck",
                status=CheckStatus.PASS,
                message="Primary key uniqueness holds"
            )
        
        status = CheckStatus.WARN
        if self.severity == DQSeverity.WARN:
            status = CheckStatus.WARN
        elif self.severity == DQSeverity.FAIL:
            status = CheckStatus.FAIL
        else:
            raise ValueError("Unrecognized severity for OHLCInvariantCheck")

    
        return CheckResult(
                check_name="UniquenessCheck",
                status=status,
                message="Duplicate primary keys detected",
                metrics = {
                    "total_rows": total,
                    "distinct_keys": distinct
                }
            )