package de.gwdg.europeanaqa.spark;

import com.jayway.jsonpath.InvalidJsonException;
import de.gwdg.europeanaqa.api.calculator.EdmCalculatorFacade;
import de.gwdg.metadataqa.api.interfaces.Calculator;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class LanguageSaturation {

	private static final Logger logger = Logger.getLogger(LanguageSaturation.class.getCanonicalName());
	private static final boolean withLabel = false;
	private static final boolean compressed = true;

	public static void main(String[] args) throws FileNotFoundException {

		if (args.length < 1) {
			System.err.println("Please provide a full path to the input files");
			System.exit(0);
		}
		if (args.length < 2) {
			System.err.println("Please provide a full path to the output file");
			System.exit(0);
		}
		String inputFileName = args[0];
		logger.log(Level.INFO, "Input file is {0}", inputFileName);
		System.err.println("Input file is " + inputFileName);
		SparkConf conf = new SparkConf().setAppName("TextLinesCount").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);

		final EdmCalculatorFacade calculator = new EdmCalculatorFacade();
		calculator.abbreviate(true);
		calculator.enableCompletenessMeasurement(false);
		calculator.enableFieldCardinalityMeasurement(false);
		calculator.enableFieldExistenceMeasurement(false);
		calculator.enableTfIdfMeasurement(false);
		calculator.enableProblemCatalogMeasurement(false);
		calculator.enableLanguageMeasurement(false);
		calculator.enableLanguageSaturationMeasurement(true);
		calculator.configure();

		logger.info("Calculators:");
		for (Calculator calc : calculator.getCalculators()) {
			logger.info(calc.getCalculatorName());
		}

		JavaRDD<String> headerRDD = context.parallelize(calculator.getHeader());
		headerRDD.saveAsTextFile("header.csv");

		/*
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
		*/
	}
}
