package de.gwdg.europeanaqa.spark.cli;

import de.gwdg.europeanaqa.api.calculator.EdmCalculatorFacade;
import de.gwdg.metadataqa.api.calculator.CalculatorFacade;
import de.gwdg.metadataqa.api.schema.MarcJsonSchema;

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

	public static CalculatorFacade createMarcCalculator() {

		final CalculatorFacade facade = new CalculatorFacade();
		// facade.abbreviate(true);
		facade.setSchema(new MarcJsonSchema());
		facade.enableCompletenessMeasurement(true);
		facade.enableFieldCardinalityMeasurement(true);
		facade.enableFieldExistenceMeasurement(true);
		facade.enableTfIdfMeasurement(false);
		facade.enableProblemCatalogMeasurement(false);
		facade.configure();

		return facade;
	}
}
