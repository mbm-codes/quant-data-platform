from pyspark.sql import DataFrame
from pyspark.sql import functions as sf

class DataQualityChecks:
    """
    Standalone class for performing data quality checks on Spark DataFrames.
    Can be easily imported and used in different pipelines.
    """

    def __init__(self, logger=None):
        self.logger = logger
        self.metrics = {}
    
    def assert_no_nulls(self, df: DataFrame, columns: list, strict=False ):
        
        null_counts = df.select([
            sf.count(sf.when(sf.col(c).isNull(), c)).alias(c) for c in columns
        ]).collect()[0].asDict()

        failed_cols = {c: count for c, count in null_counts.items() if count > 0}
        self.metrics['null_counts'] = null_counts

        if failed_cols:
            msg = f"Null value check failed for columns: {failed_cols}"
            if self.logger:
                self.logger.error(msg)
            if strict:
                raise ValueError(msg)
    
    def assert_positive_values(self, df: DataFrame, columns: list, strict=False):
        violations = {}
        for c in columns:
            count = df.filter(sf.col(c) < 0).count()
            if count > 0:
                violations[c] = count
        self.metrics['negative_values'] = violations

        if violations:
            msg = f"Negative values check found in columns: {violations}"
            if self.logger:
                self.logger.error(msg)
                if strict:
                    raise ValueError(msg)
    
    def get_metrics(self):
        return self.metrics

        
