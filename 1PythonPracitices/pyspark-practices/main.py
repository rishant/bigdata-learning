import os
import sys

from pyspark import RDD
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import IntegerType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def create_dataframe_using_tuple(spark: SparkSession) -> DataFrame:
    my_list = [1, 2, 3]
    dataframe: DataFrame = spark.createDataFrame(my_list, IntegerType())
    dataframe.show()
    return dataframe


def create_dataframe_using_textfile(spark: SparkSession) -> RDD:
    input_file_path = "file:///C://bigdata_example//day1-python-spark-practices//data//tech.txt"
    tech_rdd = spark.sparkContext.textFile(input_file_path)
    # tech_rdd = spark.sparkContext.textFile("data/tech.txt")
    print("Printing data in the tech_rdd ")
    print(tech_rdd.collect())
    return tech_rdd


def create_dataframe_using_csvfile(spark: SparkSession) -> None:
    input_file_path = "file:///C://bigdata_example//day1-python-spark-practices//data//sample.csv"
    survey_raw_df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_file_path)
    partitioned_survey_df = survey_raw_df.repartition(2)
    count_df = partitioned_survey_df.filter("Age < 40") \
        .select("Age", "Gender", "Country", "state") \
        .groupBy("Country") \
        .count()
    print("Printing data in the count_df ")
    count_df.show()


if __name__ == '__main__':
    print("Application Started...")
    print(os.getenv("PYSPARK_PYTHON"))
    print(os.getenv("PYSPARK_DRIVER_PYTHON"))

    spark = SparkSession.builder.appName("First PySpark Demo").master("local[*]").getOrCreate()

    create_dataframe_using_tuple(spark)
    create_dataframe_using_textfile(spark)
    create_dataframe_using_csvfile(spark)

    print("Application Completed")
    spark.stop()
