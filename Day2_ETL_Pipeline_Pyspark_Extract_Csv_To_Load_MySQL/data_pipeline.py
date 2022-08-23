import os
import sys
from pyspark.sql import *

import ingest
import transform
import persist

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# 1. Data-source: [CSV File | Mysql DB]
# 2. Data-ingestion/extract: [CSV File | Mysql DB] --to--> Spark DF[DataFrame] or RDD[Resilient Distributed Datasets]
# 3. Data-cleansing/formatting: Structure,Semi-Structure,UnStructure Data --transform-to--> Structure Data
# 4. Data-persist/load: Store Transformed-Structure-Data into Warehouse [CSV, Mysql]


class Pipeline:

    def __init__(self):
        self.spark = None

    def run_pipeline(self) -> None:
        print("Running Pipeline")

        # Step:1 - Ingest
        ingest_process: ingest.Ingest = ingest.Ingest(self.spark)
        df: DataFrame = ingest_process.ingest_data()
        df.show()

        # Step:2 - Transform
        transform_process: transform.Transform = transform.Transform(self.spark)
        transform_df: DataFrame = transform_process.transform_data(df)
        transform_df.show()

        # Step:3 - Persist
        persist_process: persist.Persist = persist.Persist(self.spark)
        persist_process.persist_data(transform_df)
        return

    def create_spark_session(self) -> None:
        self.spark = SparkSession \
            .builder \
            .appName("data-pipeline") \
            .config('spark.jars', 'jars/mysql-connector-java-8.0.30.jar') \
            .master('local')\
            .enableHiveSupport() \
            .getOrCreate()


if __name__ == '__main__':
    print(os.getenv("PYSPARK_PYTHON"))
    print(os.getenv("PYSPARK_DRIVER_PYTHON"))
    pipeline = Pipeline()
    pipeline.create_spark_session()
    pipeline.run_pipeline()

    # DataFrame: DataFrame is a like in memory excel.
