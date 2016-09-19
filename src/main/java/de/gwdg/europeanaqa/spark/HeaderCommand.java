package de.gwdg.europeanaqa.spark;

import de.gwdg.europeanaqa.api.calculator.EdmCalculatorFacade;
import de.gwdg.europeanaqa.spark.cli.CalculatorFacadeFactory;
import de.gwdg.metadataqa.api.interfaces.Calculator;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class HeaderCommand {

	private static final Logger logger = Logger.getLogger(HeaderCommand.class.getCanonicalName());

	public static void main(String[] args) throws FileNotFoundException {

		final EdmCalculatorFacade facade = CalculatorFacadeFactory.create();

		List<String> header = new ArrayList<>();
		for (Calculator calculator : facade.getCalculators()) {
			header.addAll(calculator.getHeader());
		}

		System.err.println(StringUtils.join(header, ","));
	}
}
