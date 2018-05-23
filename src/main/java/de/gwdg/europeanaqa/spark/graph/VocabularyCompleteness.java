package de.gwdg.europeanaqa.spark.graph;

import com.jayway.jsonpath.InvalidJsonException;
import de.gwdg.europeanaqa.api.abbreviation.EdmDataProviderManager;
import de.gwdg.europeanaqa.api.calculator.EdmCalculatorFacade;
import de.gwdg.europeanaqa.api.calculator.MultiFieldExtractor;
import de.gwdg.europeanaqa.spark.bean.Graph4PLD;
import de.gwdg.europeanaqa.spark.bean.Vocabulary;
import de.gwdg.europeanaqa.spark.cli.Parameters;
import de.gwdg.metadataqa.api.json.JsonBranch;
import de.gwdg.metadataqa.api.model.EdmFieldInstance;
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

		VocabularyCompletenessCalculator calculator = new VocabularyCompletenessCalculator(parameters.getFormat());

		JavaRDD<String> inputFile = context.textFile(inputFileName);
		JavaRDD<Vocabulary> idsRDD = inputFile
			.flatMap(jsonString -> {
					List<Vocabulary> values = new ArrayList<>();
					try {
						values = calculator.calculate(jsonString);
					} catch (InvalidJsonException e) {
						logger.severe(String.format("Invalid JSON in %s: %s. Error message: %s.",
							inputFileName, jsonString, e.getLocalizedMessage()));
					}
					return values.iterator();
				}
			);

		Dataset<Row> raw = spark.createDataFrame(idsRDD, Vocabulary.class);
		raw.cache();

		raw.select("entityType", "vocabulary", "providerId")
			.groupBy("entityType", "vocabulary")
			.count()
			.write()
			.mode(SaveMode.Overwrite)
			.csv(outputDirName + "/type-vocabulary-by-providers");

		Dataset<Row> distinct = raw.select("entityType", "vocabulary", "entityID", "cardinality").distinct();
		distinct.cache();

		Dataset<Row> vocabularies = distinct
			.select("entityType", "vocabulary", "entityID")
			.orderBy(col("entityType"), col("vocabulary"));
		vocabularies.write().mode(SaveMode.Overwrite).csv(outputDirName + "/type-vocabulary-completeness-vocabularies");

		Dataset<Row> df = distinct
			.select("entityType", "vocabulary", "cardinality")
			.orderBy(col("entityType"), col("vocabulary"));
		df.write().mode(SaveMode.Overwrite).csv(outputDirName + "/type-vocabulary-completeness-raw");

		Dataset<Row> byLinkage = raw
			.groupBy("entityType", "vocabulary", "linkage")
			.count()
			.orderBy(col("entityType"), col("vocabulary"), col("linkage").desc());
		byLinkage.write().mode(SaveMode.Overwrite).csv(outputDirName + "/type-vocabulary-completeness-by-linkage");

		Dataset<Row> counted = raw
			.groupBy("entityType", "vocabulary")
			.count();
		// counted.write().mode(SaveMode.Overwrite).csv(outputDirName + "/type-vocabulary-completeness-counted");

		Dataset<Row> ordered = counted
			.orderBy(col("entityType"), col("count").desc());

		// output every individual entity IDs with count
		ordered.write().mode(SaveMode.Overwrite).csv(outputDirName + "/type-vocabulary-completeness");
	}

	private static void help() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("java -cp [jar] de.gwdg.europeanaqa.spark.CompletenessCount [options]", options);
	}
}
