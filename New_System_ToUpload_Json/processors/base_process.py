from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class BaseProcessor(ABC):
    def __init__(self, spark):
        self.spark = spark

    @abstractmethod
    def basic_process(self, raw_df: DataFrame) -> DataFrame:
        pass

    @abstractmethod
    def refine_process(self, processed_df: DataFrame) -> DataFrame:
        pass

    @abstractmethod
    def get_output_json(self, refined_df: DataFrame, filename: str) -> dict:
        pass