package de.gwdg.europeanaqa.spark;

import de.gwdg.europeanaqa.api.calculator.EdmCalculatorFacade;
import de.gwdg.europeanaqa.spark.cli.CalculatorFacadeFactory;
import de.gwdg.europeanaqa.spark.cli.Parameters;
import de.gwdg.metadataqa.api.calculator.UniquenessCalculator;
import de.gwdg.metadataqa.api.interfaces.Calculator;
import org.apache.commons.cli.ParseException;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class UniquenessTotal {

	public static void main(String[] args) throws ParseException {

		Parameters parameters = new Parameters(args);
		final EdmCalculatorFacade facade = CalculatorFacadeFactory.createUniquenessCalculator(parameters);
		for (Calculator calculator : facade.getCalculators()) {
			if (calculator.getCalculatorName().equals(UniquenessCalculator.CALCULATOR_NAME)) {
				System.out.println(((UniquenessCalculator)calculator).getTotals());
			}
		}
	}

}
