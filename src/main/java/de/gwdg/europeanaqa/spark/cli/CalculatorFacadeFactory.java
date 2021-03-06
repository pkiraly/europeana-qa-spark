package de.gwdg.europeanaqa.spark.cli;

import de.gwdg.europeanaqa.api.calculator.EdmCalculatorFacade;
import de.gwdg.europeanaqa.api.model.Format;
import de.gwdg.metadataqa.api.calculator.CalculatorFacade;
import de.gwdg.metadataqa.api.schema.MarcJsonSchema;
import de.gwdg.metadataqa.api.schema.Schema;
import de.gwdg.metadataqa.api.util.CompressionLevel;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class CalculatorFacadeFactory {

  public static EdmCalculatorFacade createCompletenessCalculator(boolean checkSkippableCollections,
                                                                 Format format) {

    final EdmCalculatorFacade facade = new EdmCalculatorFacade();
    facade.abbreviate(true);
    facade.enableCompletenessMeasurement(true);
    facade.enableFieldCardinalityMeasurement(true);
    facade.enableFieldExistenceMeasurement(true);
    facade.enableTfIdfMeasurement(false);
    facade.enableProblemCatalogMeasurement(true);
    facade.setCheckSkippableCollections(checkSkippableCollections);
    if (format != null)
      facade.setFormat(format);
    facade.configure();

    return facade;
  }

  public static EdmCalculatorFacade createMultilingualSaturationCalculator(Parameters parameters) {

    final EdmCalculatorFacade calculator = new EdmCalculatorFacade();
    calculator.abbreviate(true);
    calculator.enableCompletenessMeasurement(false);
    calculator.enableFieldCardinalityMeasurement(false);
    calculator.enableFieldExistenceMeasurement(false);
    calculator.enableProblemCatalogMeasurement(false);
    calculator.enableTfIdfMeasurement(false);
    calculator.enableLanguageMeasurement(false);
    calculator.enableMultilingualSaturationMeasurement(true);
    calculator.setCompressionLevel(CompressionLevel.WITHOUT_TRAILING_ZEROS);
    calculator.setSaturationExtendedResult(true);
    calculator.setCheckSkippableCollections(parameters.getSkipEnrichments());
    if (parameters.getFormat() != null)
      calculator.setFormat(parameters.getFormat());

    calculator.configure();

    return calculator;
  }

  public static EdmCalculatorFacade createExtractorFacade() {

    final EdmCalculatorFacade facade = new EdmCalculatorFacade();
    facade.abbreviate(false);
    facade.enableCompletenessMeasurement(false);
    facade.enableFieldCardinalityMeasurement(false);
    facade.enableFieldExistenceMeasurement(false);
    facade.enableTfIdfMeasurement(false);
    facade.enableProblemCatalogMeasurement(false);
    facade.setCheckSkippableCollections(false);
    facade.configure();

    Schema schema = facade.getSchema();
    Map<String, String> extractableFields = new LinkedHashMap<>();
    extractableFields.put("recordId", "$.identifier");
    extractableFields.put("agent",    "$.['edm:Agent'][*]['@about']");
    extractableFields.put("concept",  "$.['skos:Concept'][*]['@about']");
    extractableFields.put("place",    "$.['edm:Place'][*]['@about']");
    extractableFields.put("timespan", "$.['edm:TimeSpan'][*]['@about']");
    schema.setExtractableFields(extractableFields);

    return facade;
  }

  public static CalculatorFacade createMarcCalculator() {

    final CalculatorFacade facade = new CalculatorFacade();
    // facade.abbreviate(true);
    facade.setSchema(new MarcJsonSchema());
    facade.enableFieldExtractor(true);
    facade.enableCompletenessMeasurement(true);
    facade.enableFieldCardinalityMeasurement(true);
    facade.enableFieldExistenceMeasurement(false);
    facade.enableTfIdfMeasurement(false);
    facade.enableProblemCatalogMeasurement(false);
    facade.configure();

    return facade;
  }

  public static EdmCalculatorFacade createLanguageCalculator(Parameters parameters) {
    final EdmCalculatorFacade calculator = new EdmCalculatorFacade();
    calculator.abbreviate(true);
    calculator.enableCompletenessMeasurement(false);
    calculator.enableFieldCardinalityMeasurement(false);
    calculator.enableFieldExistenceMeasurement(false);
    calculator.enableTfIdfMeasurement(false);
    calculator.enableProblemCatalogMeasurement(false);
    calculator.enableLanguageMeasurement(true);
    if (parameters.getFormat() != null)
      calculator.setFormat(parameters.getFormat());
    calculator.configure();
    return calculator;
  }

  public static EdmCalculatorFacade createUniquenessCalculator(Parameters parameters) {
    final EdmCalculatorFacade calculator = new EdmCalculatorFacade();
    calculator.abbreviate(true);
    calculator.enableCompletenessMeasurement(false);
    calculator.enableFieldCardinalityMeasurement(false);
    calculator.enableFieldExistenceMeasurement(false);
    calculator.enableTfIdfMeasurement(false);
    calculator.enableProblemCatalogMeasurement(false);
    calculator.enableLanguageMeasurement(false);
    calculator.enableUniquenessMeasurement(true);
    if (parameters.getFormat() != null)
      calculator.setFormat(parameters.getFormat());
    calculator.configure();
    return calculator;
  }

  public static EdmCalculatorFacade createByAnalysis(Parameters parameters) {
    EdmCalculatorFacade calculator;
    if (parameters.getAnalysis() != null) {
      switch (parameters.getAnalysis()) {
        case LANGUAGES:
          calculator = createLanguageCalculator(parameters); break;
        case MULTILINGUAL_SATURATION:
          calculator = createMultilingualSaturationCalculator(parameters); break;
        case PROXY_BASED_COMPLETENESS:
          calculator = createProxyBasedCompletenessCalculator(
            false, parameters.getFormat()
          );
          break;
        case COMPLETENESS:
        default:
          calculator = createCompletenessCalculator(
            false, parameters.getFormat()
          );
          break;
      }
    } else {
      calculator = createCompletenessCalculator(false, parameters.getFormat());
    }
    return calculator;
  }

  public static EdmCalculatorFacade createProxyBasedCompletenessCalculator(boolean checkSkippableCollections,
                                                                           Format format) {

    final EdmCalculatorFacade facade = new EdmCalculatorFacade();
    facade.abbreviate(true);
    facade.enableProxyBasedCompleteness(true);
    facade.enableCompletenessMeasurement(false);
    facade.enableFieldCardinalityMeasurement(false);
    facade.enableFieldExistenceMeasurement(false);
    facade.enableTfIdfMeasurement(false);
    facade.enableProblemCatalogMeasurement(false);
    facade.setExtendedFieldExtraction(true);
    facade.setCheckSkippableCollections(checkSkippableCollections);
    if (format != null)
      facade.setFormat(format);
    facade.configure();

    return facade;
  }

  public static EdmCalculatorFacade createAbbreviationCalculator(Format format) {

    final EdmCalculatorFacade facade = new EdmCalculatorFacade();
    facade.abbreviate(true);
    facade.enableCompletenessMeasurement(false);
    facade.enableFieldCardinalityMeasurement(false);
    facade.enableFieldExistenceMeasurement(false);
    facade.enableTfIdfMeasurement(false);
    facade.enableProblemCatalogMeasurement(false);
    facade.setCheckSkippableCollections(false);
    if (format != null)
      facade.setFormat(format);
    facade.configure();

    return facade;
  }

}
