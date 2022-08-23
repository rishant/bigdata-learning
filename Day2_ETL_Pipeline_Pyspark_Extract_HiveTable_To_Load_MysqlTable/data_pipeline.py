import os
import sys
from pyspark.sql import SparkSession

import ingest
import transform
import persist

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# 1. Data-source: Embedded derby DB (Hive Database)
# 2. Data-ingestion/extract: Hive Database --to--> Spark DF[DataFrame] or RDD[Resilient Distributed Datasets]
# 3. Data-cleansing/formatting: Structure,Semi-Structure,UnStructure Data --transform-to--> Structure Data
# 4. Data-persist/load: Store Transformed-Structure-Data into Warehouse [Hive, Mysql]


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
            .config('spark.jars.packages', 'mysql:mysql-connector-java:8.0.30,') \
            .enableHiveSupport()\
            .getOrCreate()

    # Bootstrapping Hive Table programmatically for Our testing. In realworld that will be already present in system,
    # We just need to EXTRACT in ingrest.py flow.
    def create_hive_table(self) -> None:
        self.spark.sql("create database if not exists fxxcoursedb")
        self.spark.sql("create table if not exists fxxcoursedb.fx_course_table (course_id string,course_name string,"
                       "author_name string,no_of_reviews string)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (1,'Java','FutureX',45)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (2,'Java','FutureXSkill',56)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (3,'Big Data','Future',100)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (4,'Linux','Future',100)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (5,'Microservices','Future',100)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (6,'CMS','',100)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (7,'Python','FutureX','')")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (8,'CMS','Future',56)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (9,'Dot Net','FutureXSkill',34)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (10,'Ansible','FutureX',123)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (11,'Jenkins','Future',32)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (12,'Chef','FutureX',121)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (13,'Go Lang','',105)")
        # Treat empty strings as null
        self.spark.sql("alter table fxxcoursedb.fx_course_table set tblproperties('serialization.null.format'='')")


if __name__ == '__main__':
    print(os.getenv("PYSPARK_PYTHON"))
    print(os.getenv("PYSPARK_DRIVER_PYTHON"))
    pipeline = Pipeline()
    pipeline.create_spark_session()
    # Create Hive Database & Insert some Records
    pipeline.create_hive_table()
    pipeline.run_pipeline()
