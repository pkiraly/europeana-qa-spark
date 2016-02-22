package com.nsdr.spark;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class CompletenessCount {

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
		SparkConf conf = new SparkConf().setAppName("TextLinesCount").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> inputFile = context.textFile(args[0]);
		Function<String, String> filterLinesWithSpark = new Function<String, String>() {
			public String call(String arg0) throws Exception {
				JsonPathBasedCompletenessCounter counter = new JsonPathBasedCompletenessCounter();
				counter.count(arg0);
				return counter.getCounters().getResultsAsCSV(withLabel);
				/*
				 context.write(new Text(String.format("\"%s\",%s", metadata.getDataProvider(),
				 metadata.getId().replace("http://data.europeana.eu/item/", ""))),
				 new Text());
				 */
			}
		};

		JavaRDD<String> sparkMentions = inputFile.map(filterLinesWithSpark);
		PrintWriter writer = new PrintWriter(args[1]);
		writer.println(sparkMentions.collect());
		writer.close();
	}
}
