# exceptions.py (subset)

class PipelineException(Exception):
    pass

class DQExecutionException(PipelineException):
    pass

class TransformationException(PipelineException):
    pass

class ContextException(PipelineException):
    pass
