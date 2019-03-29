package de.gwdg.europeanaqa.spark.cli;

import de.gwdg.europeanaqa.api.model.Format;
import org.apache.commons.cli.*;

import java.io.Serializable;

public class Parameters implements Serializable {

  public enum Analysis {

    COMPLETENESS("completeness"),
    LANGUAGES("languages"),
    MULTILINGUAL_SATURATION("multilingual-saturation"),
    PROXY_BASED_COMPLETENESS("proxy-based-completeness")
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
  private String recordAPIUrl;
  private Format format;
  private Analysis analysis;
  private Boolean skipEnrichments = false;
  private Boolean extendedFieldExtraction = false;
  private String mongoHost;
  private String mongoDatabase;

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
      options.addOption("a", "analysis", true, "analysis (completeness, languages, multilingual-saturation, proxy-based-completeness");
      options.addOption("e", "extendedFieldExtraction", false, "Extended field extraction");
      options.addOption("p", "recordAPIUrl", true, "URL of record API");
      options.addOption("t", "mongoHost", true, "Mongo host name");
      options.addOption("b", "mongoDatabase", true, "Mongo database name");
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
      format = Format.byCode(schemaName);
    }

    if (cmd.hasOption("analysis")) {
      String analysisName = cmd.getOptionValue("analysis");
      analysis = Analysis.byCode(analysisName);
    }

    if (cmd.hasOption("recordAPIUrl"))
      recordAPIUrl = cmd.getOptionValue("recordAPIUrl");

    skipEnrichments = cmd.hasOption("skipEnrichments");
    extendedFieldExtraction = cmd.hasOption("extendedFieldExtraction");

    if (cmd.hasOption("mongoHost"))
      mongoHost = cmd.getOptionValue("mongoHost");

    if (cmd.hasOption("mongoDatabase"))
      mongoDatabase = cmd.getOptionValue("mongoDatabase");
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

  public Format getFormat() {
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

  public String getRecordAPIUrl() {
    return recordAPIUrl;
  }

  public String getMongoHost() {
    return mongoHost;
  }

  public String getMongoDatabase() {
    return mongoDatabase;
  }
}
