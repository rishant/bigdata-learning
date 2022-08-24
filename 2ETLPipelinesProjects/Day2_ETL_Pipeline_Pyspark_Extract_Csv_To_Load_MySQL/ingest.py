from pyspark.sql import *


class Ingest:

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def ingest_data(self) -> DataFrame:
        print("Ingesting from CSV Table")
        return self.ingest_data_from_csv()
        # print("Ingesting from Mysql Table")
        # return self.ingest_data_from_mysql()

    def ingest_data_from_csv(self) -> DataFrame:
        print("Ingesting from csv")
        city_df = self.spark.read.csv("data/city.csv", header=True)
        return city_df

    def ingest_data_from_mysql(self) -> DataFrame:
        jdbc_df = self.spark.read \
            .format("jdbc") \
            .option("url", "jdbc:mysql://localhost:3306/world") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option('dbtable', 'city') \
            .option('user', 'root') \
            .option('password', 'root') \
            .load()
        return jdbc_df

