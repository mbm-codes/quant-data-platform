from enum import Enum


class CheckStatus(Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    WARN = "WARN"


class DQAction(Enum):
    NONE = "NONE"
    OBSERVE = "OBSERVE"
    CLEAN = "CLEAN"
    QUARANTINE = "QUARANTINE"
    BLOCK = "BLOCK"

class DQSeverity(Enum):
    WARN = "WARN"
    FAIL = "FAIL"  
