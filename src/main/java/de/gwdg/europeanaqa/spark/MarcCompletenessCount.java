package de.gwdg.europeanaqa.spark;

import com.jayway.jsonpath.InvalidJsonException;
import de.gwdg.europeanaqa.spark.cli.CalculatorFacadeFactory;
import de.gwdg.metadataqa.api.calculator.CalculatorFacade;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.functions;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class MarcCompletenessCount {

	private static final Logger logger = Logger.getLogger(MarcCompletenessCount.class.getCanonicalName());
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

		logger.info("Input file is " + inputFileName);
		SparkConf conf = new SparkConf().setAppName("MarcCompletenessCount");
		JavaSparkContext context = new JavaSparkContext(conf);

		final CalculatorFacade facade = CalculatorFacadeFactory.createMarcCalculator();

		JavaRDD<String> inputFile = context.textFile(inputFileName);
		Function<List<String>, String> baseCounts = new Function<List<String>, String>() {
			@Override
			public String call(List<String> fileAndJson) throws Exception {
				String fileName = fileAndJson.get(0);
				logger.info("fileName: " + fileName);
				String jsonString = fileAndJson.get(1);
				try {
					return facade.measure(jsonString);
				} catch (InvalidJsonException e) {
					logger.severe(String.format("Invalid JSON in %s: %s. Error message: %s.",
							inputFileName, jsonString, e.getLocalizedMessage()));
				}
				return "";
			}
		};

		JavaRDD<String> baseCountsRDD = inputFile.map(line -> 
				baseCounts.call(
					Arrays.asList(
						functions.input_file_name().named().name(),
						line)));
		baseCountsRDD.saveAsTextFile(outputFileName);
	}

	private static void help() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("java -cp [jar] de.gwdg.europeanaqa.spark.MarcCompletenessCount [options]", options);
	}
}
