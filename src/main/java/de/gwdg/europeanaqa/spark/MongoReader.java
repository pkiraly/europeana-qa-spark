package de.gwdg.europeanaqa.spark;

import com.mongodb.MongoClient;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.bson.codecs.BsonTypeClassMap;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

public class MongoReader {

	public static void main(final String[] args) throws InterruptedException {

		SparkSession spark = SparkSession.builder()
			.master("local")
			.appName("MongoSparkConnectorIntro")
			.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/europeana_production_publish_1.record")
			.config("spark.mongodb.input.partitioner", "MongoSplitVectorPartitioner")
			.config("spark.mongodb.input.partitionerOptions.partitionKey", "_id")
			.config("spark.mongodb.input.partitionerOptions.partitionSizeMB", "64")
			.getOrCreate();

		// Create a JavaSparkContext using the SparkSession's SparkContext object
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		// More application logic would go here...
		JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
		CodecRegistry codecRegistry = CodecRegistries.fromRegistries(MongoClient.getDefaultCodecRegistry());
		DocumentCodec codec = new DocumentCodec(codecRegistry, new BsonTypeClassMap());

		JsonWriterSettings writerSettings = new JsonWriterSettings(JsonMode.STRICT, "", "");

		// System.out.println(rdd.count());
		System.out.println(rdd.first().toJson(writerSettings, codec));

		jsc.close();
	}
}