package de.gwdg.europeanaqa.spark.cli;

import de.gwdg.europeanaqa.api.calculator.EdmCalculatorFacade;
import org.apache.commons.cli.*;

import java.io.Serializable;

public class Parameters implements Serializable {

	public enum Analysis {

		COMPLETENESS("completeness"),
		LANGUAGES("languages"),
		MULTILINGUAL_SATURATION("multilingual-saturation")
		;

		private final String name;

		private Analysis(String name) {
			this.name = name;
		}

		public static Analysis byCode(String code) {
			for(Analysis analysis : values())
				if (analysis.name.equals(code))
					return analysis;
			return null;
		}

	};

	private String inputFileName;
	private String outputFileName;
	private String headerOutputFile;
	private String dataProvidersFile;
	private String datasetsFile;
	private EdmCalculatorFacade.Formats format;
	private Analysis analysis;
	private Boolean skipEnrichments = false;
	private Boolean extendedFieldExtraction = false;

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
			options.addOption("a", "analysis", true, "format");
			options.addOption("e", "extendedFieldExtraction", false, "Extended field extraction");
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

		if (cmd.hasOption("analysis")) {
			String analysisName = cmd.getOptionValue("analysis");
			analysis = Analysis.byCode(analysisName);
		}

		skipEnrichments = cmd.hasOption("skipEnrichments");
		extendedFieldExtraction = cmd.hasOption("extendedFieldExtraction");
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

	public Boolean getExtendedFieldExtraction() {
		return extendedFieldExtraction;
	}

	public Analysis getAnalysis() {
		return analysis;
	}
}
