package de.gwdg.europeanaqa.spark.cli;

import de.gwdg.europeanaqa.api.calculator.EdmCalculatorFacade;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class CalculatorFacadeFactory {

	public static EdmCalculatorFacade create() {

		final EdmCalculatorFacade facade = new EdmCalculatorFacade();
		facade.abbreviate(true);
		facade.enableCompletenessMeasurement(true);
		facade.enableFieldCardinalityMeasurement(true);
		facade.enableFieldExistenceMeasurement(true);
		facade.enableTfIdfMeasurement(false);
		facade.enableProblemCatalogMeasurement(true);
		facade.configure();

		return facade;
	}
}
