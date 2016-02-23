package com.nsdr.spark;

import com.jayway.jsonpath.InvalidJsonException;
import java.io.FileNotFoundException;
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

	public static void main(String[] args) throws FileNotFoundException {

		if (args.length < 1) {
			System.err.println("Please provide a full path to the input files");
			System.exit(0);
		}
		if (args.length < 2) {
			System.err.println("Please provide a full path to the output file");
			System.exit(0);
		}
		logger.info("Input file is " + args[0]);
		System.err.println("Input file is " + args[0]);
		SparkConf conf = new SparkConf().setAppName("TextLinesCount").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);

		final JsonPathBasedCompletenessCounter counter = new JsonPathBasedCompletenessCounter();
		counter.setDataProviders(new DataProvidersFactory().getDataProviders());

		JavaRDD<String> inputFile = context.textFile(args[0]);
		Function<String, String> baseCounts = new Function<String, String>() {
			@Override
			public String call(String jsonString) throws Exception {
				try {
					counter.count(jsonString);
					return counter.getFullResults(withLabel);
				} catch (InvalidJsonException e) {
					System.err.println(e.getLocalizedMessage());
				}
				return "";
			}
		};

		JavaRDD<String> baseCountsRDD = inputFile.map(baseCounts);
		baseCountsRDD.saveAsTextFile(args[1]);
	}
}
