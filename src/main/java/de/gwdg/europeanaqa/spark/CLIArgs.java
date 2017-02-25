package de.gwdg.europeanaqa.spark;

import de.gwdg.europeanaqa.spark.cli.util.OptionFactory;
import java.io.FileNotFoundException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class CLIArgs {

	private static Options options = new Options();

	static {
		options.addOption(OptionFactory.create("i", "input", true, "Input file name"));
		options.addOption(OptionFactory.create("o", "output", true, "Output file name"));
		options.addOption(OptionFactory.create("h", "header", false, "Header output file name"));
		options.addOption(OptionFactory.create("p", "data-providers", false, "DataProviders file"));
		options.addOption(OptionFactory.create("s", "datasets", false, "Datasets file"));
		options.addOption(OptionFactory.create("e", "skip-enrichments", false, "Skip enriched contextual entities", false));
	}

	public static void main(String[] args) throws FileNotFoundException, ParseException {

		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Parsing failed. Reason: " + exp.getMessage());
			help();
			System.exit(0);
		}

		String inputFileName = cmd.getOptionValue("i");
		String outputFileName = cmd.getOptionValue("o");
		String headerOutputFile = cmd.getOptionValue("h");
		String dataProvidersFile = cmd.getOptionValue("data-providers");
		String datasetsFile = cmd.getOptionValue("datasets");
		boolean skipEnrichments = cmd.hasOption("skip-enrichments");

		System.err.println(String.format(
			  "inputFileName: %s, outputFileName: %s, headerOutputFile: %s, dataProvidersFile: %s, datasetsFile: %s, skipEnrichments: %s",
			  inputFileName, outputFileName, headerOutputFile, dataProvidersFile, datasetsFile, skipEnrichments
		));
	}

	private static void help() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("java -cp [jar] de.gwdg.europeanaqa.spark.CLIArgs [options]", options);
	}

}
