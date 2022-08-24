from pyspark.sql import *


class Persist:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def persist_data(self, df: DataFrame) -> None:
        print("persisting")

        # Write DataFrame into csv.
        csv_df = df
        self.write_dataframe_into_csv(csv_df)

        # Write DataFrame into Mysql table.
        mysql_df = df
        self.write_dataframe_into_mysql(mysql_df)

        # Write DataFrame into MongoDB collection.
        mongo_df = df
        self.write_dataframe_into_mongodb(mongo_df)

        # ******* mode ****** #
        # * `append`: Append contents of this :class:`DataFrame` to existing data.
        # * `overwrite`: Overwrite existing data.
        # * `error` or `errorifexists`: Throw an exception if data already exists.
        # * `ignore`: Silently ignore this operation if data already exists.

    def write_dataframe_into_csv(self, df: DataFrame) -> None:
        print("persisting dataframe into csv")
        df.write.option("header", "true").mode("overwrite").csv("output/transformed_course")

    def write_dataframe_into_mysql(self, df: DataFrame) -> None:
        print("persisting dataframe into mysql")
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:mysql://localhost:3306/transformed") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option('dbtable', 'course2') \
            .option('user', 'root') \
            .option('password', 'root') \
            .mode("overwrite") \
            .save()

    def write_dataframe_into_mongodb(self, df: DataFrame) -> None:
        print("persisting dataframe into mongodb")
        df.write\
            .format("mongodb") \
            .option("uri", "mongodb://localhost:27017") \
            .option("database", "etl_course") \
            .option("collection", "course2") \
            .mode("overwrite") \
            .save()
