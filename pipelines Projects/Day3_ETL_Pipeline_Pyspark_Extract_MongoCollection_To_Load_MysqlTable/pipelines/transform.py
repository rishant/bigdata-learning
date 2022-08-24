from pyspark.sql import *


class Transform:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def transform_data(self, df: DataFrame) -> DataFrame:
        print("transforming")
        # return df
        df1 = df.filter('course_name like "Java"')
        return df1
