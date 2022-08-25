from pyspark.sql import *


class Transform:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def transform_data(self, df: DataFrame) -> DataFrame:
        print("transforming")
        city_df = df.filter("Population > 7285000")
        # # city_df.show()
        # # city_df.describe().show()
        # # city_df.select("Population").show()
        # # city_df.groupBy("District").count().show()
        # # city_df.filter("Population > 750000").show()
        # # city_df.groupBy("District").agg({"Population": "avg", "ID": "max"}).show()
        # # city_df.orderBy("District").show()
        return city_df
