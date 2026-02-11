# errors.py (subset relevant to this pipeline)

class PipelineError(Exception):
    pass

class InputDataError(PipelineError):
    pass

class DataQualityError(PipelineError):
    def __init__(self, message, failed_checks=None):
        super().__init__(message)
        self.failed_checks = failed_checks or []

class SchemaEnforcementError(PipelineError):
    pass

class LoadError(PipelineError):
    pass

class PostETLError(PipelineError):
    pass
