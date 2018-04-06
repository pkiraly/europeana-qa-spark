package de.gwdg.europeanaqa.spark;

import com.jayway.jsonpath.InvalidJsonException;
import de.gwdg.europeanaqa.api.calculator.EdmCalculatorFacade;
import de.gwdg.europeanaqa.api.calculator.MultiFieldExtractor;
import de.gwdg.europeanaqa.spark.bean.Graph;
import de.gwdg.europeanaqa.spark.cli.Parameters;
import de.gwdg.metadataqa.api.model.JsonPathCache;
import de.gwdg.metadataqa.api.model.XmlFieldInstance;
import de.gwdg.metadataqa.api.schema.EdmFullBeanSchema;
import de.gwdg.metadataqa.api.schema.EdmOaiPmhXmlSchema;
import de.gwdg.metadataqa.api.schema.Schema;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

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

	public static void main(String[] args) throws FileNotFoundException, ParseException {

		if (args.length < 1) {
			System.err.println("Please provide a full path to the input files");
			System.exit(0);
		}
		if (args.length < 2) {
			System.err.println("Please provide a full path to the output file");
			System.exit(0);
		}

		Parameters parameters = new Parameters(args);

		final String inputFileName = parameters.getInputFileName();
		final String outputDirName = parameters.getOutputFileName();
		final boolean checkSkippableCollections = parameters.getSkipEnrichments();
			// (args.length >= 5 && args[4].equals("checkSkippableCollections"));

		logger.info("arg length: " + args.length);
		logger.info("Input file is " + inputFileName);
		logger.info("Output file is " + outputDirName);
		logger.info("checkSkippableCollections: " + checkSkippableCollections);
		System.err.println("Input file is " + inputFileName);
		SparkConf conf = new SparkConf().setAppName("GraphExtractor");
		JavaSparkContext context = new JavaSparkContext(conf);

		SparkSession spark = SparkSession.builder().getOrCreate();

		Map<String, String> extractableFields = new LinkedHashMap<>();
		Schema qaSchema = null;
		if (parameters.getFormat() == null
		    || parameters.getFormat().equals(EdmCalculatorFacade.Formats.OAI_PMH_XML)) {
			qaSchema = new EdmOaiPmhXmlSchema();
			extractableFields.put("recordId", "$.identifier");
			extractableFields.put("agent", "$.['edm:Agent'][*]['@about']");
			extractableFields.put("concept", "$.['skos:Concept'][*]['@about']");
			extractableFields.put("place", "$.['edm:Place'][*]['@about']");
			extractableFields.put("timespan", "$.['edm:TimeSpan'][*]['@about']");
		} else {
			qaSchema = new EdmFullBeanSchema();
			extractableFields.put("recordId", "$.identifier");
			extractableFields.put("agent", "$.['agents'][*]['about']");
			extractableFields.put("concept", "$.['concepts'][*]['about']");
			extractableFields.put("place", "$.['places'][*]['about']");
			extractableFields.put("timespan", "$.['timespans'][*]['about']");
		}
		qaSchema.setExtractableFields(extractableFields);

		final MultiFieldExtractor fieldExtractor = new MultiFieldExtractor(qaSchema);
		List<String> entities = Arrays.asList("agent", "concept", "place", "timespan");

		List<List<String>> statistics = new ArrayList<>();

		JavaRDD<String> inputFile = context.textFile(inputFileName);
		// statistics.add(Arrays.asList("proxy-nodes", String.valueOf(inputFile.count())));
		JavaRDD<Graph> idsRDD = inputFile
			.flatMap(jsonString -> {
					List<Graph> values = new ArrayList<>();
					try {
						JsonPathCache<? extends XmlFieldInstance> cache = new JsonPathCache<>(jsonString);
						fieldExtractor.measure(cache);
						Map<String, ? extends Object> map = fieldExtractor.getResultMap();
						String recordId = ((List<String>) map.get("recordId")).get(0);
						for (String entity : entities) {
							for (String item : (List<String>) map.get(entity)) {
								logger.info(String.format("%s, %s, %s", recordId, entity, item));
								values.add(new Graph(recordId, entity, item));
							}
						}
					} catch (InvalidJsonException e) {
						logger.severe(String.format("Invalid JSON in %s: %s. Error message: %s.",
							inputFileName, jsonString, e.getLocalizedMessage()));
					}
					return values.iterator();
				}
			);

		Dataset<Row> df = spark.createDataFrame(idsRDD, Graph.class);
		// statistics.add(Arrays.asList("entity-links", String.valueOf(df.count())));
		// context.parallelize(statistics).saveAsTextFile(outputDirName + "/statistics");

		Dataset<Row> counted = df.groupBy("type", "entityId")
			.count();
		Dataset<Row> ordered = counted.orderBy(col("type"), col("count").desc())
			//.cache()
			;

		// output every individual entity IDs with count
		ordered.write().mode(SaveMode.Overwrite).csv(outputDirName + "/type-entity-count");

		/*
		typeEntityCount
			.groupBy("type")
			.count()
			.orderBy("type")
			.write().mode(SaveMode.Overwrite).csv(outputDirName + "/entity-nodes");

		df
			.groupBy("type")
			.count()
			.orderBy("type")
			.write().mode(SaveMode.Overwrite).csv(outputDirName + "/entity-links");
		*/

	}

	private static void help() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("java -cp [jar] de.gwdg.europeanaqa.spark.CompletenessCount [options]", options);
	}
}
