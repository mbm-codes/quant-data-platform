from typing import Optional

class MetricsEmitter:
    def increment(self, name: str, value: int = 1, tags: Optional[dict] = None):
        raise NotImplementedError

    def gauge(self, name: str, value: float, tags: Optional[dict] = None):
        raise NotImplementedError

    def timing(self, name: str, value_ms: float, tags: Optional[dict] = None):
        raise NotImplementedError
