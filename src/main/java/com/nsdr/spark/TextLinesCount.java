package com.nsdr.spark;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class TextLinesCount {

	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
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
		Function<String, Boolean> filterLinesWithSpark = new Function<String, Boolean>() {
			public Boolean call(String arg0) throws Exception {
				return arg0 != null && arg0.contains("Spark");
			}
		};
		JavaRDD<String> sparkMentions = inputFile.filter(filterLinesWithSpark);
		PrintWriter writer = new PrintWriter(args[1]);
		writer.println(sparkMentions.count());
		writer.close();
	}
}
