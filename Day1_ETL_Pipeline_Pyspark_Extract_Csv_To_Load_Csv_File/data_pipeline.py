import os
import sys
from pyspark.sql import SparkSession
import ingest
import transform
import persist

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


class Pipeline:

    def __init__(self):
        self.spark = None

    def run_pipeline(self):
        print("Running Pipeline")

        # Step:1 - Ingest
        ingest_process = ingest.Ingest(self.spark)
        df = ingest_process.ingest_data()
        df.show()

        # Step:2 - Transform
        transform_process = transform.Transform(self.spark)
        transform_df = transform_process.transform_data(df)
        transform_df.show()

        # Step:3 - Persist
        persist_process = persist.Persist(self.spark)
        persist_process.persist_data(transform_df)
        return

    def create_spark_session(self):
        self.spark = SparkSession.builder.appName("data-pipeline").getOrCreate()


if __name__ == '__main__':
    print(os.getenv("PYSPARK_PYTHON"))
    print(os.getenv("PYSPARK_DRIVER_PYTHON"))
    pipeline = Pipeline()
    pipeline.create_spark_session()
    pipeline.run_pipeline()

    # DataFrame: DataFrame is a like in memory excel.
