package de.gwdg.europeanaqa.spark;

import com.jayway.jsonpath.InvalidJsonException;
import com.mongodb.MongoClient;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import de.gwdg.europeanaqa.spark.cli.CalculatorFacadeFactory;
import de.gwdg.europeanaqa.spark.cli.util.MongoRecordResolver;
import de.gwdg.metadataqa.api.calculator.CalculatorFacade;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.bson.codecs.BsonTypeClassMap;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class MongoReader  implements Serializable {

	static final Logger logger = Logger.getLogger(MongoReader.class.getCanonicalName());

	public static void main(final String[] args) throws InterruptedException {

		SparkSession spark = createSparkSession("record");
		Map<String, JavaMongoRDD<Document>> auxiliaryTables = new HashMap<>();
		String[] tableNames = new String[]{
			"agents", "concepts", "timespans", "places", "licenses",
			"aggregations", "providedCHOs", "proxies", "europeanaAggregation",
			"webResources"
		};
		for (String name : tableNames) {
			SparkSession session = createSparkSession(name);
			JavaSparkContext context = new JavaSparkContext(spark.sparkContext());
			auxiliaryTables.put(name, MongoSpark.load(context));
		}

		// Create a JavaSparkContext using the SparkSession's SparkContext object
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		MongoRecordResolver resolver = new MongoRecordResolver(auxiliaryTables);

		JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
		CodecRegistry codecRegistry = CodecRegistries.fromRegistries(
			MongoClient.getDefaultCodecRegistry());
		DocumentCodec codec = new DocumentCodec(codecRegistry, new BsonTypeClassMap());

		JsonWriterSettings writerSettings = new JsonWriterSettings(JsonMode.STRICT, "", "");

		final CalculatorFacade facade = CalculatorFacadeFactory.createMarcCalculator();

		JavaRDD<String> baseCountsRDD = rdd.map(record -> {
			resolver.resolve(record);
			String jsonString = record.toJson(writerSettings, codec);
			try {
				return facade.measure(jsonString);
			} catch (InvalidJsonException e) {
				logger.severe(String.format("Invalid JSON in %s: %s. Error message: %s.",
					jsonString, e.getLocalizedMessage()));
			}
			return "";
		});
		String outputFileName = "hdfs://localhost:54310/mongo-result";
		baseCountsRDD.saveAsTextFile(outputFileName);

		jsc.close();
	}

	private static SparkSession createSparkSession(String collection) {
		return SparkSession
			.builder()
			.master("local[*]")
			.appName("MongoSparkConnectorIntro")
			.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/")
			.config("spark.mongodb.input.database", "europeana_production_publish_1")
			.config("spark.mongodb.input.collection", collection)
			.config("spark.mongodb.input.readPreference.name", "primaryPreferred")
			.config("spark.mongodb.input.partitioner", "MongoSplitVectorPartitioner")
			.config("spark.mongodb.input.partitionerOptions.partitionKey", "_id")
			.config("spark.mongodb.input.partitionerOptions.partitionSizeMB", "64")
			.getOrCreate();
	}
}