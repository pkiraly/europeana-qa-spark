package de.gwdg.europeanaqa.spark;

import com.jayway.jsonpath.InvalidJsonException;
import de.gwdg.europeanaqa.api.calculator.EdmCalculatorFacade;
import de.gwdg.europeanaqa.spark.cli.CalculatorFacadeFactory;
import de.gwdg.europeanaqa.spark.cli.Parameters;
import de.gwdg.metadataqa.api.interfaces.Calculator;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class MultilingualSaturation {

	private static final Logger logger = Logger.getLogger(MultilingualSaturation.class.getCanonicalName());

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

		logger.log(Level.INFO, "Input file is {0}", parameters.getInputFileName());
		logger.log(Level.INFO, "Output file is {0}", parameters.getOutputFileName());
		logger.log(Level.INFO, "Header output is {0}", parameters.getHeaderOutputFile());
		logger.log(Level.INFO, "DataProviders file is {0}", parameters.getDataProvidersFile());
		logger.log(Level.INFO, "Datasets file is {0}", parameters.getDatasetsFile());
		logger.log(Level.INFO, "Skip enrichments is {0}", parameters.getSkipEnrichments());
		logger.log(Level.INFO, "Extended field extraction: {0}", parameters.getExtendedFieldExtraction());

		SparkConf conf = new SparkConf().setAppName("LanguageSaturation"); //.setMaster("local[*]");
		JavaSparkContext context = new JavaSparkContext(conf);

		final EdmCalculatorFacade calculator = CalculatorFacadeFactory
			.createMultilingualSaturationCalculator(parameters);
		calculator.setExtendedFieldExtraction(parameters.getExtendedFieldExtraction());

		logger.info("Running with the following calculators:");
		for (Calculator calc : calculator.getCalculators()) {
			logger.log(Level.INFO, "\t{0}", calc.getCalculatorName());
		}

		JavaRDD<String> headerRDD = context.parallelize(
			Arrays.asList(
				StringUtils.join(
					calculator.getHeader(), ",")));
		headerRDD.saveAsTextFile(parameters.getHeaderOutputFile());

		JavaRDD<String> inputFile = context.textFile(parameters.getInputFileName());
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
		baseCountsRDD.saveAsTextFile(parameters.getOutputFileName());

		try {
			calculator.saveDataProviders(parameters.getDataProvidersFile());
			calculator.saveDatasets(parameters.getDatasetsFile());
		} catch (UnsupportedEncodingException ex) {
			logger.severe(ex.getLocalizedMessage());
		}
	}
}
