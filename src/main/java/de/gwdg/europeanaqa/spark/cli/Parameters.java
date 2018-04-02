package de.gwdg.europeanaqa.spark.cli;

import de.gwdg.europeanaqa.api.calculator.EdmCalculatorFacade;
import org.apache.commons.cli.*;

import java.io.Serializable;

public class Parameters implements Serializable {

	private String inputFileName;
	private String outputFileName;
	private String headerOutputFile;
	private String dataProvidersFile;
	private String datasetsFile;
	private EdmCalculatorFacade.Formats format;
	private Boolean skipEnrichments = false;

	protected Options options = new Options();
	protected static CommandLineParser parser = new DefaultParser();
	protected CommandLine cmd;
	private boolean isOptionSet = false;

	protected void setOptions() {
		if (!isOptionSet) {
			options.addOption("i", "inputFileName", true, "input file name");
			options.addOption("o", "outputFileName", true, "output file name");
			options.addOption("h", "headerOutputFile", true, "header output file");
			options.addOption("d", "dataProvidersFile", true, "data providers file");
			options.addOption("c", "datasetsFile", true, "datasets file");
			options.addOption("s", "skipEnrichments", false, "skip enrichments");
			options.addOption("f", "format", true, "format");
			isOptionSet = true;
		}
	}

	public Parameters(String[] arguments)  throws ParseException {
		cmd = parser.parse(getOptions(), arguments);

		if (cmd.hasOption("inputFileName"))
			inputFileName = cmd.getOptionValue("inputFileName");

		if (cmd.hasOption("outputFileName"))
			outputFileName = cmd.getOptionValue("outputFileName");

		if (cmd.hasOption("headerOutputFile"))
			headerOutputFile = cmd.getOptionValue("headerOutputFile");

		if (cmd.hasOption("dataProvidersFile"))
			dataProvidersFile = cmd.getOptionValue("dataProvidersFile");

		if (cmd.hasOption("datasetsFile"))
			datasetsFile = cmd.getOptionValue("datasetsFile");

		if (cmd.hasOption("format")) {
			String schemaName = cmd.getOptionValue("format");
			format = EdmCalculatorFacade.Formats.byCode(schemaName);
		}

		skipEnrichments = cmd.hasOption("skipEnrichments");
	}

	public Options getOptions() {
		if (!isOptionSet)
			setOptions();
		return options;
	}

	public String getInputFileName() {
		return inputFileName;
	}

	public String getOutputFileName() {
		return outputFileName;
	}

	public String getHeaderOutputFile() {
		return headerOutputFile;
	}

	public String getDataProvidersFile() {
		return dataProvidersFile;
	}

	public String getDatasetsFile() {
		return datasetsFile;
	}

	public EdmCalculatorFacade.Formats getFormat() {
		return format;
	}

	public Boolean getSkipEnrichments() {
		return skipEnrichments;
	}
}
