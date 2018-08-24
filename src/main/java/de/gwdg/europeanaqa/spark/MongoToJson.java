package de.gwdg.europeanaqa.spark;

import com.jayway.jsonpath.InvalidJsonException;
import com.mongodb.MongoClient;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import com.mongodb.util.JSON;
import de.gwdg.europeanaqa.spark.cli.util.EuropeanaRecordReaderAPIClient;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.bson.codecs.BsonTypeClassMap;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;

import java.io.IOException;
import java.io.Serializable;
import java.util.logging.Logger;

public class MongoToJson implements Serializable {

	static final Logger logger = Logger.getLogger(MongoToJson.class.getCanonicalName());

	public static void main(final String[] args) throws InterruptedException {

		SparkSession spark = createSparkSession("record");

		// Create a JavaSparkContext using the SparkSession's SparkContext object
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
		CodecRegistry codecRegistry = CodecRegistries.fromRegistries(
			MongoClient.getDefaultCodecRegistry()
		);
		DocumentCodec codec = new DocumentCodec(codecRegistry, new BsonTypeClassMap());

		// JsonWriterSettings writerSettings = new JsonWriterSettings(JsonMode.STRICT, "", "");

		final EuropeanaRecordReaderAPIClient client = new EuropeanaRecordReaderAPIClient("144.76.218.178:8080");

		JavaRDD<String> baseCountsRDD = rdd.map(record -> {
			TaskContext tc = TaskContext.get();

			if (tc.partitionId() == 2
			    || tc.partitionId() == 413
			    || tc.partitionId() > 1238) {
				String jsonFragment = JSON.serialize(record);
				String id = record.get("about", String.class);
				try {
					String jsonString = client.resolveFragmentWithPost(jsonFragment, id);
					logger.info(id + ": " + jsonString.length());
					return jsonString;
					// return id;
				} catch (IOException e) {
					logger.severe(
						String.format(
							"Resolving error. Id: %s, fragment in %s: %s. Error message: %s.",
							id, jsonFragment, e.getLocalizedMessage()));
				} catch (InvalidJsonException e) {
					String jsonString = "";
					logger.severe(String.format("Invalid JSON in %s: %s. Error message: %s.",
						jsonString, e.getLocalizedMessage()));
				}
			}
			return "";
		});
		String outputFileName = "hdfs://localhost:54310/mongo-result2";
		baseCountsRDD
			.filter(record -> !record.equals(""))
			.saveAsTextFile(outputFileName);

		jsc.close();
	}

	private static SparkSession createSparkSession(String collection) {
		return SparkSession
			.builder()
			// .master("local[*]")
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