

class MetricsEmitter:
    def increment(self, name: str, value: int = 1, tags: dict | None = None):
        raise NotImplementedError

    def gauge(self, name: str, value: float, tags: dict | None = None):
        raise NotImplementedError

    def timing(self, name: str, value_ms: float, tags: dict | None = None):
        raise NotImplementedError
