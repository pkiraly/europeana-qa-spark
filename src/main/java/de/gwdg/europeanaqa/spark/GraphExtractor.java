package de.gwdg.europeanaqa.spark;

import com.jayway.jsonpath.InvalidJsonException;
import de.gwdg.europeanaqa.api.calculator.EdmFieldExtractor;
import de.gwdg.europeanaqa.api.calculator.MultiFieldExtractor;
import de.gwdg.metadataqa.api.model.JsonPathCache;
import de.gwdg.metadataqa.api.model.XmlFieldInstance;
import de.gwdg.metadataqa.api.schema.EdmOaiPmhXmlSchema;
import de.gwdg.metadataqa.api.schema.Schema;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.FileNotFoundException;
import java.util.*;
import java.util.logging.Logger;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class GraphExtractor {

	private static final Logger logger = Logger.getLogger(GraphExtractor.class.getCanonicalName());
	private static Options options = new Options();

	public static void main(String[] args) throws FileNotFoundException {

		if (args.length < 1) {
			System.err.println("Please provide a full path to the input files");
			System.exit(0);
		}
		if (args.length < 2) {
			System.err.println("Please provide a full path to the output file");
			System.exit(0);
		}
		final String inputFileName = args[0];
		final String outputFileName = args[1];
		final boolean checkSkippableCollections = (args.length >= 5 && args[4].equals("checkSkippableCollections"));

		logger.info("arg length: " + args.length);
		logger.info("Input file is " + inputFileName);
		logger.info("Output file is " + outputFileName);
		logger.info("checkSkippableCollections: " + checkSkippableCollections);
		System.err.println("Input file is " + inputFileName);
		SparkConf conf = new SparkConf().setAppName("CompletenessCount");
		JavaSparkContext context = new JavaSparkContext(conf);

		// SparkSession session = SparkSession.builder().getOrCreate();
		// SQLContext sqlContext = new SQLContext(context);

		Schema schema = new EdmOaiPmhXmlSchema();
		Map<String, String> extractableFields = new LinkedHashMap<>();
		extractableFields.put("recordId", "$.identifier");
		extractableFields.put("agent",    "$.['edm:Agent'][*]['@about']");
		extractableFields.put("concept",  "$.['skos:Concept'][*]['@about']");
		extractableFields.put("place",    "$.['edm:Place'][*]['@about']");
		extractableFields.put("timespan", "$.['edm:TimeSpan'][*]['@about']");
		schema.setExtractableFields(extractableFields);

		final MultiFieldExtractor fieldExtractor = new MultiFieldExtractor(schema);
		List<String> entities = Arrays.asList("agent", "concept", "place", "timespan");

		JavaRDD<String> inputFile = context.textFile(inputFileName);
		JavaRDD<List<String>> idsRDD = inputFile
			.flatMap(jsonString -> {
					List<List<String>> values = new ArrayList<>();
					try {
						JsonPathCache<? extends XmlFieldInstance> cache = new JsonPathCache<>(jsonString);
						fieldExtractor.measure(cache);
						Map<String, ? extends Object> map = fieldExtractor.getResultMap();
						String recordId = (String) map.get("recordId");
						for (String entity : entities) {
							for (String item : (List<String>) map.get(entity)) {
								values.add(Arrays.asList(recordId, entity, item));
							}
						}
					} catch (InvalidJsonException e) {
						logger.severe(String.format("Invalid JSON in %s: %s. Error message: %s.",
							inputFileName, jsonString, e.getLocalizedMessage()));
					}
					return values.iterator();
				}
			);
		/*
		List<StructField> fields = Arrays.asList(
			DataTypes.createStructField("recordId", DataTypes.StringType, true),
			DataTypes.createStructField("type", DataTypes.StringType, true),
			DataTypes.createStructField("entityId", DataTypes.StringType, true)
		);
		StructType dfSchema = DataTypes.createStructType(fields);
		DataFrame df = session.createDataFrame(idsRDD, dfSchema);
		// DataFrame df = sqlContext.createDataFrame(idsRDD, dfSchema);
		*/

		idsRDD.saveAsTextFile(outputFileName);
	}

	private static void help() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("java -cp [jar] de.gwdg.europeanaqa.spark.CompletenessCount [options]", options);
	}
}
