from pyspark.sql import *


class Transform:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def transform_data(self, df: DataFrame) -> DataFrame:
        print("transforming")

        # null or blank author_name field filled with "Unknown"
        df1 = df.na.fill("Unknown", ["author_name"])

        # null or blank no_of_reviews field filled with "0"
        df2 = df1.na.fill("0", ["no_of_reviews"])

        return df2
