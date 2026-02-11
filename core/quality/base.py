from abc import ABC, abstractmethod
from core.quality.results import CheckResult

class DataQualityCheck(ABC):
    
    @abstractmethod
    def run(self, context) -> CheckResult:
        """
        Execute the data quality check.
        Context is intentionally generic.
        """
        pass

    

