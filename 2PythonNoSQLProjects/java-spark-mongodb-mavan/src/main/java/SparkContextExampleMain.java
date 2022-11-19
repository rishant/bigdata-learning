import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;

public class SparkContextExampleMain {

	public static void main(String[] args) {
		/*
		 * Create the SparkSession. If config arguments are passed from the command line
		 * using --conf, parse args for the values to set.
		 */
		SparkSession spark = SparkSession.builder()
				.master("local")
				.appName("MongoSparkConnectorIntro")
				.config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/test.myCollection")
				.config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/test.myCollection")
				.getOrCreate();
				
		Dataset<Row> df = spark.read().format("com.mongodb.spark.sql.DefaultSource")
				.option("database", "test")
				.option("collection", "spark")
				.load();
		df.printSchema();
		df.show();
		
//		Dataset<Row> df = spark.read().format("json").load("example.json");
//		df.write().format("com.mongodb.spark.sql.DefaultSource").mode("overwrite").save();

		// Read Config
//		Map<String, String> readOverrides = new HashMap<>();
//	    readOverrides.put("collection", "characters");
//	    readOverrides.put("readPreference.name", "secondaryPreferred");
//	    ReadConfig readConfig = ReadConfig.create(spark).withOptions(readOverrides);

	    // Load data with implicit schema
//	    Dataset<Row> implicitDS = MongoSpark.load(spark, readConfig, Row.class).toDF();
//	    implicitDS.printSchema();
//	    implicitDS.show();
//
//		implicitDS.createOrReplaceTempView("characters");
//		Dataset<Row> centenarians = spark.sql("SELECT name, age FROM characters WHERE age >= 100");
//		centenarians.show();
	}
}
