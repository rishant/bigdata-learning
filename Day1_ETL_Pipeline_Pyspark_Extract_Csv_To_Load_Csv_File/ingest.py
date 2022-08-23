class Ingest:

    def __init__(self, spark):
        self.spark = spark

    def ingest_data(self):
        print("Ingesting")
        return self.ingest_data_from_csv()

    def ingest_data_from_csv(self):
        print("Ingesting from csv")
        retail_store_df = self.spark.read.csv("data/retailstore.csv", header=True)
        return retail_store_df

