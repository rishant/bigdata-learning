import os
import sys
from pyspark.sql import SparkSession

import ingest
import transform
import persist

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# 1. Data-source: [Mysql DB]
# 2. Data-ingestion/extract: [Mysql DB] --to--> Spark DF[DataFrame] or RDD[Resilient Distributed Datasets]
# 3. Data-cleansing/formatting: Structure,Semi-Structure,UnStructure Data --transform-to--> Structure Data
# 4. Data-persist/load: Store Transformed-Structure-Data into Warehouse [Cassandra DB]


class Pipeline:

    def __init__(self):
        self.spark = None

    def run_pipeline(self) -> None:
        print("Running Pipeline")

        # Step:1 - Ingest
        ingest_process = ingest.Ingest(self.spark)
        course_df = ingest_process.ingest_data()
        course_df.show()

        # Step:2 - Transform
        transform_process = transform.Transform(self.spark)
        transform_course_df = transform_process.transform_data(course_df)
        transform_course_df.show()

        # Step:3 - Persist
        persist_process = persist.Persist(self.spark)
        persist_process.persist_data(transform_course_df)
        return

    def create_spark_session(self) -> None:
        self.spark = SparkSession\
            .builder\
            .appName("data-pipeline") \
            .master("local") \
            .config('spark.mongodb.input.uri', 'mongodb://localhost:27017/etl_course.course') \
            .config('spark.mongodb.output.uri', 'mongodb://localhost:27017/etl_course.course') \
            .config('spark.jars.packages', 'mysql:mysql-connector-java:8.0.30,'
                                           'org.mongodb.spark:mongo-spark-connector:10.0.3,'
                                           'com.datastax.spark:spark-cassandra-connector_2.12:3.1.0') \
            .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        self.spark.conf.set("spark.sql.catalog.cassandra", "com.datastax.spark.connector.datasource.CassandraCatalog")
        # * Testing catalog name can be any name like: spark.sql.catalog.mycatalog | spark.sql.catalog.pysparklog
        self.spark.sql("SHOW NAMESPACES FROM cassandra").show()


if __name__ == '__main__':
    print(os.getenv("PYSPARK_PYTHON"))
    print(os.getenv("PYSPARK_DRIVER_PYTHON"))
    pipeline = Pipeline()
    pipeline.create_spark_session()
    pipeline.run_pipeline()
