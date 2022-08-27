import pyspark
from pyspark.sql import SparkSession


class Persist:

    def __init__(self, spark):
        self.spark = spark

    def persist_data(self, df):
        print("persisting")
        df.write.option("header", "true").csv("output/etl_course")
