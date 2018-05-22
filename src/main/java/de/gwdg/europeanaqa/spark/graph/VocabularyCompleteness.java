package de.gwdg.europeanaqa.spark.graph;

import com.jayway.jsonpath.InvalidJsonException;
import de.gwdg.europeanaqa.api.abbreviation.EdmDataProviderManager;
import de.gwdg.europeanaqa.api.calculator.EdmCalculatorFacade;
import de.gwdg.europeanaqa.api.calculator.MultiFieldExtractor;
import de.gwdg.europeanaqa.spark.bean.Graph4PLD;
import de.gwdg.europeanaqa.spark.bean.Vocabulary;
import de.gwdg.europeanaqa.spark.cli.Parameters;
import de.gwdg.metadataqa.api.model.JsonPathCache;
import de.gwdg.metadataqa.api.model.XmlFieldInstance;
import de.gwdg.metadataqa.api.schema.EdmFullBeanSchema;
import de.gwdg.metadataqa.api.schema.EdmOaiPmhXmlSchema;
import de.gwdg.metadataqa.api.schema.Schema;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.FileNotFoundException;
import java.util.*;
import java.util.logging.Logger;

import static org.apache.spark.sql.functions.col;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class VocabularyCompleteness {

	private static final Logger logger = Logger.getLogger(VocabularyCompleteness.class.getCanonicalName());
	private static Options options = new Options();
	private static final EdmDataProviderManager dataProviderManager = new EdmDataProviderManager();

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
			// extractableFields.put("dataProvider", "$.['ore:Aggregation'][0]['edm:dataProvider'][0]");
			// extractableFields.put("provider", "$.['ore:Aggregation'][0]['edm:provider'][0]");
			extractableFields.put("agent", "$.['edm:Agent'][*]['@about']");
			extractableFields.put("concept", "$.['skos:Concept'][*]['@about']");
			extractableFields.put("place", "$.['edm:Place'][*]['@about']");
			extractableFields.put("timespan", "$.['edm:TimeSpan'][*]['@about']");
		} else {
			qaSchema = new EdmFullBeanSchema();
			extractableFields.put("recordId", "$.identifier");
			// extractableFields.put("dataProvider", "$.['aggregations'][0]['edmDataProvider'][0]");
			// extractableFields.put("provider", "$.['aggregations'][0]['edmProvider'][0]");
			extractableFields.put("agent", "$.['agents'][*]['about']");
			extractableFields.put("concept", "$.['concepts'][*]['about']");
			extractableFields.put("place", "$.['places'][*]['about']");
			extractableFields.put("timespan", "$.['timespans'][*]['about']");
		}
		qaSchema.setExtractableFields(extractableFields);

		final MultiFieldExtractor fieldExtractor = new MultiFieldExtractor(qaSchema);
		final VocabularyExtractor vocabularyExtractor = new VocabularyExtractor(qaSchema);
		List<String> entities = Arrays.asList("agent", "concept", "place", "timespan");

		List<List<String>> statistics = new ArrayList<>();
		Map<String, String> cardinalityMap = new HashMap<>();
		Map<String, String> vocabNameMap = new HashMap<>();

		JavaRDD<String> inputFile = context.textFile(inputFileName);
		// statistics.add(Arrays.asList("proxy-nodes", String.valueOf(inputFile.count())));
		JavaRDD<Vocabulary> idsRDD = inputFile
			.flatMap(jsonString -> {
					List<Vocabulary> values = new ArrayList<>();
					try {
						JsonPathCache<? extends XmlFieldInstance> cache = new JsonPathCache<>(jsonString);
						fieldExtractor.measure(cache);
						Map<String, ? extends Object> map = fieldExtractor.getResultMap();
						String recordId = ((List<String>) map.get("recordId")).get(0);

						/*
						String dataProvider = extractValue(map, "dataProvider");
						String provider = extractValue(map, "provider");

						String providerId = (dataProvider != null)
							? getDataProviderCode(dataProvider)
							: (provider != null ? getDataProviderCode(provider) : "0");
						*/

						for (String entityType : entities) {
							for (String entityID : (List<String>) map.get(entityType)) {
								String cardinality = null;
								if (cardinalityMap.containsKey(entityID)) {
									cardinality = cardinalityMap.get(entityID);
								} else {
									List<Integer> cardinalities = vocabularyExtractor.getCardinality(cache, entityType, entityID);
									cardinality = StringUtils.join(cardinalities, ",");
									cardinalityMap.put(entityID, cardinality);
								}
								String vocabulary = null;
								if (vocabNameMap.containsKey(entityID)) {
									vocabulary = vocabNameMap.get(entityID);
								} else {
									vocabulary = VocabularyUtils.extractPLD(entityID);
									if (vocabulary.equals(entityID)) {
										logger.severe(String.format("%s -- Undetected vocabulary (%s): %s", recordId, entityType, entityID));
									}
									vocabNameMap.put(entityID, vocabulary);
								}

								values.add(new Vocabulary(
									entityType,
									entityID,
									vocabulary,
									cardinality
								));
							}
						}
					} catch (InvalidJsonException e) {
						logger.severe(String.format("Invalid JSON in %s: %s. Error message: %s.",
							inputFileName, jsonString, e.getLocalizedMessage()));
					}
					return values.iterator();
				}
			);

		Dataset<Row> raw = spark.createDataFrame(idsRDD, Vocabulary.class).distinct();
		raw.cache();
		Dataset<Row> vocabularies = raw
			.select("entityType", "vocabulary", "entityID")
			.orderBy(col("entityType"), col("vocabulary"));
		vocabularies.write().mode(SaveMode.Overwrite).csv(outputDirName + "/type-vocabulary-completeness-vocabularies");

		Dataset<Row> df = raw.select("entityType", "vocabulary", "cardinality")
			.orderBy(col("entityType"), col("vocabulary"));
		df.write().mode(SaveMode.Overwrite).csv(outputDirName + "/type-vocabulary-completeness-raw");

		/*
		Dataset<Row> counted = df
										.groupBy("type", "vocabulary")
										.count();
		counted.write().mode(SaveMode.Overwrite).csv(outputDirName + "/type-vocabulary-completeness-counted");

		Dataset<Row> ordered = counted
										.orderBy(col("type"), col("count").desc());

		// output every individual entity IDs with count
		ordered.write().mode(SaveMode.Overwrite).csv(outputDirName + "/type-vocabulary-completeness");
		*/
	}

	public static String getDataProviderCode(String dataProvider) {
		String dataProviderCode;
		if (dataProvider == null) {
			dataProviderCode = "0";
		} else if (dataProviderManager != null) {
			dataProviderCode = String.valueOf(dataProviderManager.lookup(dataProvider));
		} else {
			dataProviderCode = dataProvider;
		}
		return dataProviderCode;
	}

	private static String extractValue(Map map, String key) {
		String value = null;
		if (map.get(key) != null && !((List<String>) map.get(key)).isEmpty())
			value = ((List<String>) map.get(key)).get(0);
		return value;
	}

	private static void help() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("java -cp [jar] de.gwdg.europeanaqa.spark.CompletenessCount [options]", options);
	}
}
