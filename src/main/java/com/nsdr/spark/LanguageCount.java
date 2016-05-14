package com.nsdr.spark;

import com.nsdr.spark.completeness.DatasetManager;
import com.nsdr.spark.completeness.DataProviderManager;
import com.jayway.jsonpath.InvalidJsonException;
import com.nsdr.spark.completeness.LanguageCalculator;
import com.nsdr.spark.counters.Counters;
import com.nsdr.spark.model.JsonPathCache;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.logging.Logger;
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
		logger.info("Input file is " + inputFileName);
		System.err.println("Input file is " + inputFileName);
		SparkConf conf = new SparkConf().setAppName("TextLinesCount").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);

		final LanguageCalculator languageCalculator = new LanguageCalculator();
		DataProviderManager dataProviderManager = new DataProviderManager();
		languageCalculator.setDataProviderManager(dataProviderManager);
		DatasetManager datasetManager = new DatasetManager();
		languageCalculator.setDatasetManager(datasetManager);

		JavaRDD<String> inputFile = context.textFile(inputFileName);
		Function<String, String> baseCounts = new Function<String, String>() {
			@Override
			public String call(String jsonString) throws Exception {
				try {
					JsonPathCache cache = new JsonPathCache(jsonString);
					Counters counters = new Counters();
					languageCalculator.calculate(cache, counters);
					return languageCalculator.getResult();
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
			dataProviderManager.save(args[2]);
			datasetManager.save(args[3]);
		} catch (UnsupportedEncodingException ex) {
			logger.severe(ex.getLocalizedMessage());
		}
	}
}
