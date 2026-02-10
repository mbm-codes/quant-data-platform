from core.quality.table_checks import UniquenessCheck
from core.quality.enums import CheckStatus, DQSeverity, DQAction

def build_table_checks(df, cfg: list):
    checks = []

    for check in cfg:
        if not check.get("enabled", True):
            continue
            
        name = check["name"]
        try:
            severity = DQSeverity(check.get("severity", DQSeverity.WARN))
            action_on_warn = DQAction(check.get("action_on_warn", DQAction.NONE))
            action_on_fail = DQAction(check.get("action_on_fail", DQAction.NONE))
            mostly = check.get("mostly", 0.8)
        except ValueError:
            raise ValueError(f"Invalid CheckStatus: {check.get('severity')}")

        if name == "primary_key_uniqueness":
            key_cols = check.get("columns", [])
            checks.append(UniquenessCheck(
                df=df,
                key_columns=key_cols,
                severity=severity,
                action_on_warn=action_on_warn,
                action_on_fail=action_on_fail,
                mostly=mostly
            ))
        else:
            raise ValueError(f"Unknown table check: {name}")

    return checks