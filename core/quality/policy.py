from core.quality.enums import CheckStatus, DQSeverity, DQAction

DQ_ACTION_MATRIX = {
    (CheckStatus.PASS, DQSeverity.WARN): DQAction.OBSERVE,
    (CheckStatus.PASS, DQSeverity.FAIL): DQAction.OBSERVE,
    (CheckStatus.FAIL, DQSeverity.WARN): DQAction.QUARANTINE,
    (CheckStatus.FAIL, DQSeverity.FAIL): DQAction.BLOCK,
    (CheckStatus.WARN, DQSeverity.WARN): DQAction.OBSERVE,
    (CheckStatus.WARN, DQSeverity.FAIL): DQAction.QUARANTINE,

}

DQ_ACTION_PRIORITY = {
    DQAction.NONE: 0,
    DQAction.OBSERVE: 1,
    DQAction.QUARANTINE: 2,
    DQAction.CLEAN: 3,
    DQAction.BLOCK: 4
}

def decide_action(status, severity, action_on_warn, action_on_fail) -> DQAction:
    
    if status == CheckStatus.WARN:
        if DQ_ACTION_PRIORITY[DQ_ACTION_MATRIX[(status, severity)]] >= DQ_ACTION_PRIORITY[action_on_warn]:
            return DQ_ACTION_MATRIX[(status, severity)]
        else:
            return action_on_warn
    elif status == CheckStatus.FAIL:
        if DQ_ACTION_PRIORITY[DQ_ACTION_MATRIX[(status, severity)]] >= DQ_ACTION_PRIORITY[action_on_fail]:
            return DQ_ACTION_MATRIX[(status, severity)]
        else:
            return action_on_fail
    else:
        return DQAction.OBSERVE


