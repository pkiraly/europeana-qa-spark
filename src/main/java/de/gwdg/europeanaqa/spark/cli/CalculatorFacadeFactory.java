package de.gwdg.europeanaqa.spark.cli;

import de.gwdg.europeanaqa.api.calculator.EdmCalculatorFacade;
import de.gwdg.metadataqa.api.calculator.CalculatorFacade;
import de.gwdg.metadataqa.api.json.JsonBranch;
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

	public static EdmCalculatorFacade create(boolean checkSkippableCollections) {

		final EdmCalculatorFacade facade = new EdmCalculatorFacade();
		facade.abbreviate(true);
		facade.enableCompletenessMeasurement(true);
		facade.enableFieldCardinalityMeasurement(true);
		facade.enableFieldExistenceMeasurement(true);
		facade.enableTfIdfMeasurement(false);
		facade.enableProblemCatalogMeasurement(true);
		facade.setCheckSkippableCollections(checkSkippableCollections);
		facade.configure();

		return facade;
	}

	public static EdmCalculatorFacade createMultilingualSaturationCalculator(boolean skipEnrichments, boolean useFullBeanFormat) {

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
		calculator.setCheckSkippableCollections(skipEnrichments);
		if (useFullBeanFormat)
			calculator.setFormat(EdmCalculatorFacade.Formats.FULLBEAN);

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
}
