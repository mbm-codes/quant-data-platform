from typing import List, Optional, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, greatest, least

from core.quality.base import DataQualityCheck
from core.quality.results import CheckResult
from core.quality.enums import CheckStatus, DQAction, DQSeverity
from core.quality.policy import DQ_ACTION_MATRIX, decide_action

class NotNullCheck(DataQualityCheck):
    """
    Ensures a column contains no NULL values.
    """

    def __init__(
            self, df: DataFrame, 
            column: str, 
            severity: DQSeverity, 
            mostly: float, 
            action_on_warn: DQAction, 
            action_on_fail: DQAction
        ):
        
        self.df = df
        self.column = column
        self.severity = severity
        self.mostly = mostly
        self.action_on_warn = action_on_warn
        self.action_on_fail = action_on_fail

    def run(self, context: Optional[Dict[str, Any]] = None) -> CheckResult:
        total_count = self.df.count()
        null_count = self.df.filter(col(self.column).isNull()).count()

        if null_count == 0:
            return CheckResult(
                check_name=f"NotNullCheck({self.column})",
                status=CheckStatus.PASS,
                action=DQAction.OBSERVE,
                message="No null values found",
                metrics={
                    "row_count": total_count,
                    "null_count": null_count,
                    "null_ratio": null_count / total_count if total_count > 0 else 0.0
                },
                invalid_predicate=f"{self.column} IS NULL"
            )

        null_ratio = null_count / total_count if total_count > 0 else 0
        
        status = CheckStatus.WARN if null_ratio >= self.mostly else CheckStatus.FAIL

        dq_action = decide_action(status, self.severity, self.action_on_warn, self.action_on_fail)

        return CheckResult(
            check_name=f"NotNullCheck({self.column})",
            status=status,
            action=dq_action,
            message=f"{null_count} null values found",
            invalid_predicate=f"{self.column} IS NULL",
            metrics={
                "row_count": total_count,
                "null_count": null_count,
                "null_ratio": null_count / total_count if total_count > 0 else 0.0
            }
        )


class ValueRangeCheck(DataQualityCheck):
    """
    Validates that values in a column fall within a specified numeric range,
    with support for null handling, inclusivity, tolerances, conditional filters,
    and severity control.
    """

    def __init__(
        self,
        df: DataFrame,
        column: str,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
        *,
        inclusive_min: bool = True,
        inclusive_max: bool = True,
        allow_null: bool = False,
        mostly: float = 1.0,
        where: Optional[str] = None,
        severity: DQSeverity = DQSeverity.FAIL,
        message: Optional[str] = None,
        cast: Optional[str] = None,
        strict_type: bool = False,
        sample_percent: int = 100,
        tags: Optional[list[str]] = None,
        owner: Optional[str] = None,
        action_on_warn: Optional[DQAction] = DQAction.NONE,
        action_on_fail: Optional[DQAction] = DQAction.NONE
    ):
        if mostly <= 0 or mostly > 1:
            raise ValueError("mostly must be in the range (0, 1].")

        self.df = df
        self.column = column
        self.min_val = min_val
        self.max_val = max_val
        self.inclusive_min = inclusive_min
        self.inclusive_max = inclusive_max
        self.allow_null = allow_null
        self.mostly = mostly
        self.where = where
        self.severity = severity
        self.message = message
        self.cast = cast
        self.strict_type = strict_type
        self.sample_percent = sample_percent
        self.tags = tags or []
        self.owner = owner
        self.action_on_warn = action_on_warn
        self.action_on_fail = action_on_fail

    def run(self, context: Optional[Dict[str, Any]] = None) -> CheckResult:
        df = self.df

        # Apply conditional filter        
        if self.where:
            df = df.filter(self.where)

        # Optional sampling (for very large datasets)
        if self.sample_percent < 100:
            df = df.sample(self.sample_percent / 100.0)

        total_count = df.count()
        if total_count == 0:
            return CheckResult(
                check_name=f"ValueRangeCheck({self.column})",
                status=CheckStatus.PASS,
                action=DQAction.OBSERVE,
                message="No rows to validate",
                metrics={
                "row_count": float(total_count),
                "valid_count": float(0),
                "invalid_count": float(0),
                "valid_ratio": float(0),
                "mostly": self.mostly,
                "sample_percent": float(self.sample_percent),
                }
            )

        column_expr = col(self.column)


        # Build range condition
        conditions = []

        if self.min_val is not None:
            op = ">=" if self.inclusive_min else ">"
            conditions.append(f"{self.column} {op} {self.min_val}")

        if self.max_val is not None:
            op = "<=" if self.inclusive_max else "<"
            conditions.append(f"{self.column} {op} {self.max_val}")

        range_condition = " AND ".join(conditions)

        # Null handling
        if not self.allow_null:
            range_condition = f"{range_condition} AND {self.column} IS NOT NULL"
        else:
            range_condition = f"({range_condition}) OR {self.column} IS NULL"

        valid_count = df.filter(range_condition).count()
        valid_ratio = valid_count / total_count

        warn = valid_ratio >= self.mostly
        if warn:
            status = CheckStatus.WARN
        else:
            status = CheckStatus.FAIL
        
        dq_action = decide_action(status, self.severity, self.action_on_warn, self.action_on_fail)        

        default_message = (
            f"{valid_ratio:.2%} of values in '{self.column}' "
            f"are within the expected range"
        )

        if self.where:
            inv_predicate = f"NOT({self.where} AND {range_condition})"
        else:
            inv_predicate = f"NOT({range_condition})"

        return CheckResult(
            check_name=f"ValueRangeCheck({self.column})",
            status=status,
            action=dq_action,
            message=self.message or default_message,
            metrics={
                "row_count": float(total_count),
                "valid_count": float(valid_count),
                "invalid_count": float(total_count - valid_count),
                "valid_ratio": valid_ratio,
                "mostly": self.mostly,
                "sample_percent": float(self.sample_percent),
            },
            invalid_predicate=inv_predicate
        )

class OHLCInvariantCheck(DataQualityCheck):
    def __init__(
            self,
            df: DataFrame,
            *,
            open_col: str = "open",
            high_col: str = "high",
            low_col: str = "low",
            close_col: str = "close",
            allow_null: bool = False,
            mostly: float = 1.0,
            where: Optional[str] = None,
            severity: DQSeverity = DQSeverity.FAIL,
            message: Optional[str] = None,
            sample_percent: int = 100,
            tags: Optional[List[str]] = None,
            owner: Optional[str] = None,
            action_on_warn: Optional[DQAction] = DQAction.NONE,
            action_on_fail: Optional[DQAction] = DQAction.NONE,
    ):
    
        if mostly <= 0 or mostly > 1:
            raise ValueError("mostly must be in the range (0,1].)")

        self.df = df
        self.open_col = open_col
        self.high_col = high_col
        self.low_col = low_col
        self.close_col = close_col
        self.allow_null = allow_null
        self.mostly = mostly
        self.where = where
        self.severity = severity
        self.message = message
        self.sample_percent = sample_percent
        self.tags = tags or []
        self.owner = owner
        self.action_on_warn = action_on_warn
        self.action_on_fail = action_on_fail

    def run(self, context: Optional[Dict[str, Any]] = None) -> CheckResult:
        df = self.df

        # Conditional filter
        if self.where:
            df = df.filter(self.where)
        
        # Sampling (large datasets)
        if self.sample_percent < 100:
            df = df.sample(self.sample_percent / 100.0)

        total_count = df.count()
        if total_count == 0:
            return CheckResult(
                check_name="OHLCInvariantCheck",
                status=CheckStatus.PASS,
                action=decide_action(CheckStatus.PASS, self.severity, self.action_on_warn, self.action_on_fail),
                message="No rows to validate",
                metrics={
                    "row_count": float(total_count),
                    "valid_count": float(0),
                    "invalid_count": float(0),
                    "valid_ratio": float(0),
                    "mostly": self.mostly,
                    "sample_percent": float(self.sample_percent)
                },
            )

        open_c = col(self.open_col)
        high_c = col(self.high_col)
        low_c = col(self.low_col)
        close_c = col(self.close_col)

        # Core invariants 
        invariant_condition = (
            (high_c >= greatest(open_c, close_c)) &
            (low_c <= least(open_c, close_c)) & 
            (high_c >= low_c)
        )

        # Null handling
        if not self.allow_null:
            invariant_condition = (
                invariant_condition & 
                open_c.isNotNull() &
                high_c.isNotNull() &
                low_c.isNotNull()
            )

        valid_count = df.filter(invariant_condition).count()
        valid_ratio = valid_count / total_count

        warn = valid_ratio >= self.mostly

        if warn:
            status = CheckStatus.WARN
        else:
            status = CheckStatus.FAIL

        dq_action = decide_action(status, self.severity, self.action_on_warn, self.action_on_fail)

        inv_invariant_condition = f"""
            (({self.high_col} >= greatest({self.open_col}, {self.close_col})) AND
            ({self.low_col} <= least({self.open_col}, {self.close_col})) AND
            ({self.high_col} >= {self.low_col}))
        """

        if not self.allow_null:
            inv_predicate = f"NOT({inv_invariant_condition} AND ({self.open_col} IS NOT NULL AND {self.high_col} IS NOT NULL AND {self.close_col} IS NOT NULL AND {self.low_col} IS NOT NULL))" 
        else:
            inv_predicate = f"NOT({inv_invariant_condition})"

        default_message = (
            f"{valid_ratio: .2%} of rows satisfy OHLC invariants "
            f"(high >= max(open, close), low <= min(open, close), high >= low)"
        )

        return CheckResult(
            check_name="OHLCInvariantcheck",
            status=status,
            action=dq_action,
            message=self.message or default_message,
            metrics={
                "row_count": float(total_count),
                "valid_count": float(valid_count),
                "invalid_count": float(total_count - valid_count),
                "valid_ratio": valid_ratio,
                "mostly": self.mostly,
                "sample_percent": float(self.sample_percent)
            },
            details= {
                "open_col": self.open_col,
                "high_col": self.high_col,
                "low_col": self.low_col,
                "close_col": self.close_col,
                "allow_null": self.allow_null,
                "tags": self.tags,
                "owner": self.owner,

            },
            invalid_predicate=inv_predicate
        )