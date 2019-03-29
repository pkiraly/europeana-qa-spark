package de.gwdg.europeanaqa.spark;

import com.jayway.jsonpath.InvalidJsonException;
import com.mongodb.MongoClient;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import com.mongodb.util.JSON;
import de.gwdg.europeanaqa.spark.cli.Parameters;
import de.gwdg.europeanaqa.spark.cli.util.EuropeanaRecordReaderAPIClient;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
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

  public static void main(final String[] args)
      throws InterruptedException, ParseException {

    Parameters parameters = new Parameters(args);
    if (StringUtils.isBlank(parameters.getOutputFileName())) {
      System.err.println("Please provide a full path to the output file");
      System.exit(0);
    }

    if (StringUtils.isBlank(parameters.getRecordAPIUrl())) {
      System.err.println("Please provide a URL of the record API");
      System.exit(0);
    }

    SparkSession spark = createSparkSession(parameters.getMongoHost(), parameters.getMongoDatabase(), "record");

    // Create a JavaSparkContext using the SparkSession's SparkContext object
    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

    JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
    CodecRegistry defaultRegistry = MongoClient.getDefaultCodecRegistry();
    CodecRegistry codecRegistry = CodecRegistries.fromRegistries(defaultRegistry);
    DocumentCodec codec = new DocumentCodec(codecRegistry, new BsonTypeClassMap());

    // JsonWriterSettings writerSettings = new JsonWriterSettings(JsonMode.STRICT, "", "");

    // "144.76.218.178:8080"
    final EuropeanaRecordReaderAPIClient client = new EuropeanaRecordReaderAPIClient(
      parameters.getRecordAPIUrl()
    );

    JavaRDD<String> baseCountsRDD = rdd.map(record -> {
      TaskContext tc = TaskContext.get();
      // tc.stageId();
      // int partitionId = tc.partitionId();

      // if (isProcessable(partitionId)) {
        String jsonFragment = JSON.serialize(record);
        String id = record.get("about", String.class);
        try {
          String jsonString = client.resolveFragmentWithPost(jsonFragment, id);
          // logger.info(partitionId + ") " + id + ": " + jsonString.length());
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
      // }
      return "";
    });

    // String outputFileName = "hdfs://localhost:54310/mongo-result2";
    baseCountsRDD
      .filter(record -> !record.equals(""))
      .saveAsTextFile(parameters.getOutputFileName());

    jsc.close();
  }

  private static SparkSession createSparkSession(String mongoHost, String database, String collection) {
    return SparkSession
      .builder()
      // .master("local[*]")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", String.format("mongodb://%s/", mongoHost)) // 127.0.0.1
      .config("spark.mongodb.input.database", database) // "europeana_production_publish_1"
      .config("spark.mongodb.input.collection", collection)
      .config("spark.mongodb.input.readPreference.name", "primaryPreferred")
      .config("spark.mongodb.input.partitioner", "MongoSplitVectorPartitioner")
      .config("spark.mongodb.input.partitionerOptions.partitionKey", "_id")
      .config("spark.mongodb.input.partitionerOptions.partitionSizeMB", "64")
      .getOrCreate();
  }

  private static boolean isProcessable(int partitionId) {
    return (partitionId == 2 || partitionId == 413 || partitionId > 1238);
  }
}