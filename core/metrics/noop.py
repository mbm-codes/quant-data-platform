from core.metrics.emitter import MetricsEmitter

class NoOpMetricsEmitter(MetricsEmitter):
    def increment(self, *args, **kwargs): pass
    def gauge(self, *args, **kwargs): pass
    def timing(self, *args, **kwargs): pass
