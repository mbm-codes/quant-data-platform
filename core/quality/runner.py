from core.quality.results import CheckStatus

class DataQualityRunner:
    def __init__(self, checks: list, logger=None):
        self.checks = checks
        self.logger = logger
    def run(self):
        results = []
        for check in self.checks:
            result = check.run()
            results.append(result)

            if result.status == CheckStatus.FAIL:
                #configurable later: fail fast vs continue
                if self.logger:
                    self.logger.error(f"[FAIL] {result.check_name}: {result.message}")
                raise ValueError(f"{result.check_name} check failed, hence failing the execution")
            elif result.status == CheckStatus.WARN:
                if self.logger:
                    self.logger.warning(f"[WARN] {result.check_name}: {result.message} ")
            else:
                if self.logger:
                    self.logger.info(f"[{result.status.value}] {result.check_name}")
        return results