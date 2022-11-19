import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.bson.Document;
import static java.util.Arrays.asList;

import static java.util.Collections.singletonList;

import java.util.HashMap;
import java.util.Map;

public class JavaSparkContextExampleMain {

	public static void main(final String[] args) {
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
		
		// Create a JavaSparkContext using the SparkSession's SparkContext object
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		// More application logic would go here...

		writeDataIntoDB(jsc);
	    readDataFromDB(jsc);
	    
	    aggregationPipelineFromDB(jsc);
	    
		etlUsingMongodbSQLQuery(spark, jsc);

		jsc.close();
	}

	private static void etlUsingMongodbSQLQuery(SparkSession spark, JavaSparkContext jsc) {
		// Read Config
		Map<String, String> readOverrides = new HashMap<>();
	    readOverrides.put("collection", "characters");
	    readOverrides.put("readPreference.name", "secondaryPreferred");
	    ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

	    // Load data with implicit schema
	    Dataset<Row> implicitDS = MongoSpark.load(jsc, readConfig).toDF();
	    implicitDS.printSchema();
	    implicitDS.show();
	    
	    // Load data with explicit schema
	    Dataset<Character> explicitDS = MongoSpark.load(jsc, readConfig).toDS(Character.class);
	    explicitDS.printSchema();
	    explicitDS.show();
	    
	    // Create the temp view and execute the query
	    explicitDS.createOrReplaceTempView("characters");
	    Dataset<Row> centenarians = spark.sql("SELECT name, age FROM characters WHERE age >= 100");
	    centenarians.show();
	    centenarians.filter(centenarians.col("age").$greater$eq(200)).show();
	    
	    // Write the data to the "hundredClub" collection
	    MongoSpark.write(centenarians).option("collection", "hundredClub").mode("overwrite").save();
	    
	    // Load the data from the "hundredClub" collection
	    MongoSpark.load(spark, ReadConfig.create(spark).withOption("collection", "hundredClub"), Character.class).show();
	}

	private static void aggregationPipelineFromDB(JavaSparkContext jsc) {
		
		Map<String, String> readOverrides = new HashMap<>();
	    readOverrides.put("collection", "spark");
	    readOverrides.put("readPreference.name", "secondaryPreferred");
	    ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
	    
	    // Load and analyze data from MongoDB
	    JavaMongoRDD<Document> rdd = MongoSpark.load(jsc, readConfig);
	    
	    /*Start Example: Use aggregation to filter a RDD***************/
	    JavaMongoRDD<Document> aggregatedRdd = rdd.withPipeline(
	      singletonList(
	        Document.parse("{ $match: { spark : { $gt : 5 } } }")));
	    /*End Example**************************************************/
	    // Analyze data from MongoDB
	    System.out.println(aggregatedRdd.count());
	    System.out.println(aggregatedRdd.first().toJson());
	    aggregatedRdd.collect().forEach(doc -> System.out.println(doc.toJson()));
	    
	}

	private static void readDataFromDB(JavaSparkContext jsc) {
		
		/*Start Example: Read data from MongoDB************************/
	    // Create a custom ReadConfig
	    Map<String, String> readOverrides = new HashMap<>();
	    readOverrides.put("collection", "spark");
	    readOverrides.put("readPreference.name", "secondaryPreferred");
	    ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
	    
	    // Load data using the custom ReadConfig
	    JavaMongoRDD<Document> customRdd = MongoSpark.load(jsc, readConfig);
	    /*End Example**************************************************/
	    
	    // Analyze data from MongoDB
	    System.out.println(customRdd.count());
	    System.out.println(customRdd.first().toJson());
	    customRdd.collect().forEach(doc -> System.out.println(doc.toJson()));
	    
	}

	private static void writeDataIntoDB(JavaSparkContext jsc) {
		
		// Create a custom WriteConfig
		Map<String, String> writeOverrides = new HashMap<>();
		writeOverrides.put("collection", "spark");
		writeOverrides.put("writeConcern.w", "majority");
		WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);

		// Create a RDD of 10 documents
		JavaRDD<Document> sparkDocuments = jsc.parallelize(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
				.map(new Function<Integer, Document>() {
					public Document call(final Integer i) throws Exception {
						return Document.parse("{spark: " + i + "}");
					}
				});

		/* Start Example: Save data from RDD to MongoDB *****************/
		MongoSpark.save(sparkDocuments, writeConfig);
		/* End Example **************************************************/
		
	}
}
