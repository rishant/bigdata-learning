from pyspark import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *

my_spark = SparkSession\
   .builder \
   .appName("PyApp")\
   .config("spark.mongodb.input.uri", "mongodb://localhost:27017/etl_course.course")\
   .config("spark.mongodb.output.uri", "mongodb://localhost:27017/etl_course.course") \
   .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector:10.0.3')\
   .getOrCreate()

upload_df = my_spark.read.format("mongodb")\
    .option("uri", "mongodb://localhost:27017") \
    .option("database", "proton") \
    .option("collection", "upload") \
    .option("replaceDocument", "true")\
    .load()
upload_df.printSchema()
print(upload_df.schema.json())
upload_df.show()
upload_df.filter(upload_df['logType'] == 'DUI').show()

upload_df.createOrReplaceTempView("upload")
sqlDF = my_spark.sql("SELECT logType, fileId, userName from upload")
sqlDF.show()
# https://sparkbyexamples.com/spark/spark-sql-structtype-on-dataframe/

my_spark.stop()