from abc import abstractmethod
from pyspark.sql import DataFrame
from core.pipeline.base import PipelineBase


class IngestionBase(PipelineBase):
    """
    Base class for ingestion -> bronze pipelines
    """

    def extract(self) -> DataFrame: 
        return self.read_source()
    
    def transform(self, df_or_dfs, ctx) -> DataFrame:
        if isinstance(df_or_dfs, DataFrame):
            df = df_or_dfs
        elif isinstance(df_or_dfs, dict):
            df = next(iter(df_or_dfs.values()))
        else:
            raise ValueError("df_or_dfs must be a DataFrame or dict")
        
        df = self.standardize(df)
        df = self.cast_types(df)
        self.run_data_quality_checks(df, ctx)
        return df

    def load(self, df: DataFrame):
        self.write_bronze(df)

    #---- Abstract ingestion-specific methods ---

    @abstractmethod
    def read_source(self) -> DataFrame:
        pass
    
    @abstractmethod
    def standardize(self, df: DataFrame) -> DataFrame:
        pass

    @abstractmethod
    def cast_types(self, df: DataFrame) -> DataFrame:
        pass
    
    @abstractmethod
    def write_bronze(self, df: DataFrame):
        pass

    #---- Optional ----

    def run_data_quality_checks(self, df: DataFrame, ctx):
        pass