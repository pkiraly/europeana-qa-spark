package de.gwdg.europeanaqa.spark;

import com.jayway.jsonpath.InvalidJsonException;
import de.gwdg.europeanaqa.api.calculator.EdmCalculatorFacade;
import de.gwdg.europeanaqa.spark.cli.Parameters;
import de.gwdg.metadataqa.api.interfaces.Calculator;
import de.gwdg.metadataqa.api.util.CompressionLevel;
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

		/*
		String inputFileName = args[0];
		String outputFileName = args[1];
		String headerOutputFile = args[2];
		String dataProvidersFile = args[3];
		String datasetsFile = args[4];
		boolean skipEnrichments = (args.length >= 6 && args[5].equals("skip-enrichments"));
		*/

		String inputFileName = parameters.getInputFileName();
		String outputFileName = parameters.getOutputFileName();
		String headerOutputFile = parameters.getHeaderOutputFile();
		String dataProvidersFile = parameters.getDataProvidersFile();
		String datasetsFile = parameters.getDatasetsFile();
		EdmCalculatorFacade.Formats format = parameters.getFormat();
		boolean skipEnrichments = parameters.getSkipEnrichments();

		logger.log(Level.INFO, "Input file is {0}", inputFileName);
		logger.log(Level.INFO, "Output file is {0}", outputFileName);
		logger.log(Level.INFO, "Header output is {0}", headerOutputFile);
		logger.log(Level.INFO, "DataProviders file is {0}", dataProvidersFile);
		logger.log(Level.INFO, "Datasets file is {0}", datasetsFile);
		logger.log(Level.INFO, "Skip enrichments is {0}", skipEnrichments);

		SparkConf conf = new SparkConf().setAppName("LanguageSaturation"); //.setMaster("local[*]");
		JavaSparkContext context = new JavaSparkContext(conf);

		final EdmCalculatorFacade calculator = new EdmCalculatorFacade();
		calculator.abbreviate(true);
		calculator.enableCompletenessMeasurement(false);
		calculator.enableFieldCardinalityMeasurement(false);
		calculator.enableFieldExistenceMeasurement(false);
		calculator.enableTfIdfMeasurement(false);
		calculator.enableProblemCatalogMeasurement(false);
		calculator.enableLanguageMeasurement(false);
		calculator.enableMultilingualSaturationMeasurement(true);
		calculator.setCompressionLevel(CompressionLevel.WITHOUT_TRAILING_ZEROS);
		calculator.setSaturationExtendedResult(true);
		calculator.setCheckSkippableCollections(skipEnrichments);
		if (format != null)
			calculator.setFormat(format);

		calculator.configure();

		logger.info("Running with the following calculators:");
		for (Calculator calc : calculator.getCalculators()) {
			logger.log(Level.INFO, "\t{0}", calc.getCalculatorName());
		}

		JavaRDD<String> headerRDD = context.parallelize(
			Arrays.asList(
				StringUtils.join(
					calculator.getHeader(), ",")));
		headerRDD.saveAsTextFile(headerOutputFile);

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
		baseCountsRDD.saveAsTextFile(outputFileName);

		try {
			calculator.saveDataProviders(dataProvidersFile);
			calculator.saveDatasets(datasetsFile);
		} catch (UnsupportedEncodingException ex) {
			logger.severe(ex.getLocalizedMessage());
		}
	}
}
