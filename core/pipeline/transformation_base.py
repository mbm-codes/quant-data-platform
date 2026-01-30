from abc import ABC, abstractmethod
from core.pipeline.base import PipelineBase
from pyspark.sql import DataFrame
from typing import Union

class SilverPipelineBase(PipelineBase):
    """
    Base class for all bronze -> silver pipelines
    Inherits from PipelineBase to reuse lifecycle, logging and hooks
    """

    def extract(self) -> Union[DataFrame, dict]:
        return self.read_bronze()

    def transform(self, df_or_dfs, ctx) -> DataFrame:
        if isinstance(df_or_dfs, DataFrame):
            df = df_or_dfs
        elif isinstance(df_or_dfs, dict):
            df = df_or_dfs["first"]
        else:
            raise ValueError("df_or_dfs must be a DataFrame or dict")

        df = self.apply_business_rules(df)
        self.run_quality_checks(df, ctx)
        return df

    def load(self, df: DataFrame):
        self.write_silver(df)

    # Abstract methonds for concerte implementation
    @abstractmethod
    def read_bronze(self) -> Union[DataFrame, dict]:
        pass

    @abstractmethod
    def read_silver(self) -> Union[DataFrame, dict]:
        pass

    @abstractmethod
    def apply_business_rules(self, df: DataFrame) -> DataFrame:
        pass

    @abstractmethod
    def write_silver(self, df: DataFrame):
        pass

    #optional hook
    def run_quality_checks(self, df: DataFrame, ctx):
        pass
