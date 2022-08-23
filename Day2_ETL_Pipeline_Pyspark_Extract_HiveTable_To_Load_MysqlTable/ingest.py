from pyspark.sql import *


class Ingest:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def ingest_data(self):
        print("Ingesting from Hive Table")
        course_df = self.spark.sql("select * from fxxcoursedb.fx_course_table")
        return course_df
