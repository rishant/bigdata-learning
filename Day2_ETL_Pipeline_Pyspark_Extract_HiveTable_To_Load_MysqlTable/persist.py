from pyspark.sql import *


class Persist:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def persist_data(self, df: DataFrame) -> None:
        print("persisting")
        # Write DataFrame into Mysql table.
        self.write_dataframe_into_mysql(df)
        # Write DataFrame into Hive table.
        self.write_dataframe_into_hive(df)

    def write_dataframe_into_mysql(self, df: DataFrame) -> None:
        print("Write DataFrame into MySQL")
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:mysql://localhost:3306/transformed") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option('dbtable', 'course') \
            .option('user', 'root') \
            .option('password', 'root') \
            .mode('overwrite')\
            .save()

    def write_dataframe_into_hive(self, df: DataFrame) -> None:
        print("Write DataFrame into [Temp] Hive Table.")
        df.createOrReplaceTempView("temp_table")
        self.spark.sql("CREATE TABLE IF NOT EXISTS fxxcoursedb.fx_course_transformed_table as select * from temp_table").show()
