package de.gwdg.europeanaqa.spark;

import com.jayway.jsonpath.InvalidJsonException;
import de.gwdg.europeanaqa.api.calculator.EdmCalculatorFacade;
import de.gwdg.europeanaqa.spark.cli.CalculatorFacadeFactory;
import de.gwdg.europeanaqa.spark.cli.Parameters;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.FileNotFoundException;
import java.util.logging.Logger;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class ProxyBasedCompletenessCount {

	private static final Logger logger = Logger.getLogger(ProxyBasedCompletenessCount.class.getCanonicalName());
	private static Options options = new Options();

	public static void main(String[] args)
			throws FileNotFoundException, ParseException {

		if (args.length < 1) {
			System.err.println("Please provide a full path to the input files");
			System.exit(0);
		}
		if (args.length < 2) {
			System.err.println("Please provide a full path to the output file");
			System.exit(0);
		}
		Parameters parameters = new Parameters(args);

		logger.info("arg length: " + args.length);
		logger.info("Input file is " + parameters.getInputFileName());
		logger.info("Output file is " + parameters.getOutputFileName());
		logger.info("data providers file: " + parameters.getDataProvidersFile());
		logger.info("datasets file: " + parameters.getDatasetsFile());
		logger.info("format: " + parameters.getFormat());
		logger.info("check skippable collections: " + parameters.getSkipEnrichments());
		logger.info("Extended field extraction: " + parameters.getExtendedFieldExtraction());

		SparkConf conf = new SparkConf().setAppName("CompletenessCount"); //.setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);

		final EdmCalculatorFacade facade = CalculatorFacadeFactory.createProxyBasedCompletenessCalculator(
			parameters.getSkipEnrichments(), parameters.getFormat()
		);
		facade.setExtendedFieldExtraction(parameters.getExtendedFieldExtraction());

		JavaRDD<String> inputFile = context.textFile(parameters.getInputFileName());
		Function<String, String> baseCounts = new Function<String, String>() {
			@Override
			public String call(String jsonString) throws Exception {
				try {
					return facade.measure(jsonString);
				} catch (InvalidJsonException e) {
					logger.severe(String.format("Invalid JSON in %s: %s. Error message: %s.",
						parameters.getInputFileName(), jsonString, e.getLocalizedMessage()));
				}
				return "";
			}
		};

		JavaRDD<String> baseCountsRDD = inputFile.map(baseCounts);
		baseCountsRDD.saveAsTextFile(parameters.getOutputFileName());

	}

	private static void help() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("java -cp [jar] de.gwdg.europeanaqa.spark.ProxyBasedCompletenessCount [options]", options);
	}
}
