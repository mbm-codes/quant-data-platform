from core.quality.results import CheckStatus, DQAction

class DataQualityRunner:
    def __init__(self, checks: list, logger=None):
        self.checks = checks
        self.logger = logger
    def run(self):
        results = []
        for check in self.checks:
            result = check.run()
            results.append(result)

            if self.logger:
                if result.status == CheckStatus.WARN:
                    self.logger.warning(f"[WARN] {result.check_name}: {result.message}")
                elif result.status == CheckStatus.FAIL:
                    self.logger.error(f"[FAIL] {result.check_name}: {result.message}")
                else:
                    self.logger.info(f"[INFO] {result.check_name}: {result.message}")
        return results