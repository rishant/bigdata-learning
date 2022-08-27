class Ingest:

    def __init__(self, spark):
        self.spark = spark

    def ingest_data(self):
        print("Ingesting")
        return self.ingest_data_using_jdbc()

    def ingest_data_using_jdbc(self):
        print("Ingesting using jdbc")
        retail_store_df = self.spark.read.format('jdbc') \
            .options(url="jdbc:mysql://localhost:3306", driver="com.mysql.cj.jdbc.Driver", user="root", password="root") \
            .option("query", "select * from etl_course.course2") \
            .load()
        return retail_store_df

