from pyspark.sql import *


class Ingest:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def ingest_data(self):
        print("Ingesting")
        # course_df = self.ingest_data_using_csv()
        # course_df = self.ingest_data_using_mysql()
        course_df = self.ingest_data_using_mongodb()
        return course_df

    def ingest_data_using_csv(self):
        print("Ingesting from csv")
        course_csv_df = self.spark.read.csv("data/course.csv", header=True)
        return course_csv_df

    def ingest_data_using_mysql(self):
        mysql_df = self.spark.read\
            .format("jdbc") \
            .option("url", "jdbc:mysql://localhost:3306/transformed") \
            .option('driver', 'com.mysql.cj.jdbc.Driver') \
            .option('user', 'root') \
            .option('password', 'root') \
            .option('query', 'select * from transformed.course')\
            .load()
        return mysql_df

    def ingest_data_using_mongodb(self):
        print("Ingesting from Mongo Collection")
        course_df = self.spark.read.\
            format("mongodb") \
            .option("uri", "mongodb://localhost:27017") \
            .option("database", "etl_course") \
            .option("collection", "course") \
            .load()
        course_df.printSchema()
        return course_df
