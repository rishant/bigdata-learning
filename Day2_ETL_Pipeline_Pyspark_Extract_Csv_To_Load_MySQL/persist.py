from pyspark.sql import *


class Persist:

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def persist_data(self, df: DataFrame) -> None:
        print("persisting")
        # Write transformed DataFrame into Mysql table.
        self.write_dataframe_into_csv(df)
        # Write transformed DataFrame into Mysql table.
        self.write_dataframe_into_mysql(df)

    def write_dataframe_into_csv(self, df: DataFrame) -> None:
        df1 = df.coalesce(1)
        df1.write.option("header", "true").csv("output/transformed_city")

    def write_dataframe_into_mysql(self, df: DataFrame) -> None:
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:mysql://localhost:3306/transformed") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option('dbtable', 'transformed_city') \
            .option('user', 'root') \
            .option('password', 'root') \
            .save()

