package de.gwdg.europeanaqa.spark;

import com.jayway.jsonpath.InvalidJsonException;
import de.gwdg.europeanaqa.api.calculator.EdmCalculatorFacade;
import de.gwdg.europeanaqa.spark.cli.CalculatorFacadeFactory;
import de.gwdg.europeanaqa.spark.cli.Parameters;
import de.gwdg.metadataqa.api.calculator.UniquenessCalculator;
import de.gwdg.metadataqa.api.interfaces.Calculator;
import org.apache.commons.cli.ParseException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.logging.Logger;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class UniquenessTotal {

	private static final Logger logger = Logger.getLogger(UniquenessTotal.class.getCanonicalName());
	private static final boolean withLabel = false;
	private static final boolean compressed = true;

	public static void main(String[] args) throws FileNotFoundException, ParseException {

		if (args.length < 1) {
			System.err.println("Please provide a full path to the input files");
			System.exit(0);
		}
		if (args.length < 2) {
			System.err.println("Please provide a full path to the output file");
			System.exit(0);
		}

		Parameters parameters = new Parameters(args);
		final EdmCalculatorFacade facade = CalculatorFacadeFactory.createUniquenessCalculator(parameters);
		for (Calculator calculator : facade.getCalculators()) {
			if (calculator.getCalculatorName().equals(UniquenessCalculator.CALCULATOR_NAME)) {
				System.out.println(((UniquenessCalculator)calculator).getTotals());
			}
		}
	}

}
