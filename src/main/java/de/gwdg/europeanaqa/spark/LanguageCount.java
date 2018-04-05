package de.gwdg.europeanaqa.spark;

import com.jayway.jsonpath.InvalidJsonException;
import de.gwdg.europeanaqa.api.calculator.EdmCalculatorFacade;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.logging.Logger;

import de.gwdg.europeanaqa.spark.cli.Parameters;
import org.apache.commons.cli.ParseException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class LanguageCount {

	private static final Logger logger = Logger.getLogger(LanguageCount.class.getCanonicalName());
	private static final boolean withLabel = false;
	private static final boolean compressed = true;

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

		String inputFileName = parameters.getInputFileName();
		logger.info("Input file is " + inputFileName);
		SparkConf conf = new SparkConf().setAppName("LanguageCount");
		JavaSparkContext context = new JavaSparkContext(conf);

		final EdmCalculatorFacade calculator = new EdmCalculatorFacade();
		calculator.abbreviate(true);
		calculator.enableCompletenessMeasurement(false);
		calculator.enableFieldCardinalityMeasurement(false);
		calculator.enableFieldExistenceMeasurement(false);
		calculator.enableTfIdfMeasurement(false);
		calculator.enableProblemCatalogMeasurement(false);
		calculator.enableLanguageMeasurement(true);
		if (parameters.getFormat() != null)
			calculator.setFormat(parameters.getFormat());
		calculator.configure();

		JavaRDD<String> inputFile = context.textFile(inputFileName);
		Function<String, String> baseCounts = new Function<String, String>() {
			@Override
			public String call(String jsonString) throws Exception {
				try {
					return calculator.measure(jsonString);
				} catch (InvalidJsonException e) {
					logger.severe(String.format("Invalid JSON in %s. Error message: %s.",
							jsonString, e.getLocalizedMessage()));
				}
				return "";
			}
		};

		JavaRDD<String> baseCountsRDD = inputFile.map(baseCounts);
		baseCountsRDD.saveAsTextFile(args[1]);

		try {
			calculator.saveDataProviders(args[2]);
			calculator.saveDatasets(args[3]);
		} catch (UnsupportedEncodingException ex) {
			logger.severe(ex.getLocalizedMessage());
		}
	}
}
