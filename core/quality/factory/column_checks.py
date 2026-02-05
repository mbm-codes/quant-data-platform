from core.quality.column_checks import NotNullCheck, ValueRangeCheck, OHLCInvariantCheck
from core.quality.enums import CheckStatus, DQSeverity

def build_column_checks(df, cfg: list):
    checks = []

    for check in cfg:
        if not check.get("enabled", True):
            continue
            
        name = check["name"]

        try:
            severity = DQSeverity(check.get("severity", DQSeverity.WARN))
        except ValueError:
            raise ValueError(f"Invalid CheckStatus: {check.get('severity')}, Check: {name}")


        if name == "not_null":
            for col in check.get("columns", []):
                checks.append(NotNullCheck(df, col, severity))
        elif name == "value_range":
            checks.append(ValueRangeCheck(
                df=df,
                column=check.get("column", ""),
                min_val=check.get("min", None),
                max_val=check.get("max", None),
                inclusive_min=check.get("inclusive_min"),
                inclusive_max=check.get("inclusive_max"),
                allow_null=check.get("allow_null"),
                mostly=check.get("mostly"),
                where=check.get("where"),
                severity=severity,
                message=check.get("message"),
                cast=check.get("cast"),
                strict_type=check.get("strict_type"),
                sample_percent=check.get("sample_percent"),
                tags=check.get("tags"),
                owner=check.get("owner"),
            ))
        elif name == "ohlc_invariant":
            checks.append(OHLCInvariantCheck(
                df=df,
                open_col=check.get("open_col"),
                high_col=check.get("high_col"),
                low_col=check.get("low_col"),
                close_col=check.get("close_col"),
                allow_null=check.get("allow_null"),
                mostly=check.get("mostly"),
                where=check.get("where"),
                severity=severity,
                tags=check.get("tags"),
                owner=check.get("owner"),
            ))
        else:
            raise ValueError(f"Unknown column check: {name}")

    return checks