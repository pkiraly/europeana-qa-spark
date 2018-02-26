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

import java.util.logging.Logger;

public class MongoReader {

	static final Logger logger = Logger.getLogger(MongoReader.class.getCanonicalName());

	public static void main(final String[] args) throws InterruptedException {

		SparkSession spark = SparkSession.builder()
			.master("local[*]")
			.appName("MongoSparkConnectorIntro")
			.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/europeana_production_publish_1.record")
			.config("spark.mongodb.input.partitioner", "MongoSplitVectorPartitioner")
			.config("spark.mongodb.input.partitionerOptions.partitionKey", "_id")
			.config("spark.mongodb.input.partitionerOptions.partitionSizeMB", "64")
			.getOrCreate();

		// Create a JavaSparkContext using the SparkSession's SparkContext object
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		MongoRecordResolver resolver = new MongoRecordResolver("127.0.0.1", 27017, "europeana_production_publish_1");

		JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
		CodecRegistry codecRegistry = CodecRegistries.fromRegistries(MongoClient.getDefaultCodecRegistry());
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
}