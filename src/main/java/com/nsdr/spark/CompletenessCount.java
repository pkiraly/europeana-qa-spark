package com.nsdr.spark;

import com.jayway.jsonpath.InvalidJsonException;
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
public class CompletenessCount {

	private static Logger logger = Logger.getLogger(CompletenessCount.class.getCanonicalName());
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

		final JsonPathBasedCompletenessCounter counter = new JsonPathBasedCompletenessCounter();
		DataProviderManager dataProviderManager = new DataProviderManager();
		counter.setDataProviderManager(dataProviderManager);
		DatasetManager datasetManager = new DatasetManager();
		counter.setDatasetManager(datasetManager);
		counter.setInputFileName(inputFileName);

		JavaRDD<String> inputFile = context.textFile(inputFileName);
		Function<String, String> baseCounts = new Function<String, String>() {
			@Override
			public String call(String jsonString) throws Exception {
				
				try {
					counter.count(jsonString);
					return counter.getFullResults(withLabel, compressed);
				} catch (InvalidJsonException e) {
					System.err.println(e.getLocalizedMessage());
					logger.severe(String.format("Invalid JSON in %s: %s. Error message: %s.", 
							  counter.getInputFileName(), jsonString, e.getLocalizedMessage()));
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
