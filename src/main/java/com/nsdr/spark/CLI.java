package com.nsdr.spark;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.nsdr.europeanaqa.api.calculator.EdmCalculatorFacade;
import com.nsdr.spark.cli.Result;
import java.io.IOException;
import java.util.logging.Logger;
import org.codehaus.jackson.map.ObjectMapper;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class CLI {

	private static final Logger LOGGER = Logger.getLogger(CLI.class.getCanonicalName());

	private static Cluster cluster;
	private static Session session;

	public static void main(String[] args) {

		if (args.length < 1) {
			System.err.println("Please provide a record identifier");
			System.exit(0);
		}
		String id = args[0];

		cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		session = cluster.connect("europeana");

		final EdmCalculatorFacade calculator = new EdmCalculatorFacade();
		calculator.doAbbreviate(true);
		calculator.runCompleteness(true);
		calculator.runFieldCardinality(true);
		calculator.runFieldExistence(true);
		calculator.runTfIdf(true);
		calculator.runProblemCatalog(true);
		calculator.collectTfIdfTerms(true);
		calculator.configure();

		// final CompletenessCalculator completenessCalculator = new CompletenessCalculator();
		// completenessCalculator.setVerbose(true);
		
		// final TfIdfCalculator tfIdfCalculator = new TfIdfCalculator();
		// tfIdfCalculator.setDoCollectTerms(true);
		// DataProviderManager dataProviderManager = new DataProviderManager();
		// counter.setDataProviderManager(dataProviderManager);
		// DatasetManager datasetManager = new DatasetManager();
		// counter.setDatasetManager(datasetManager);

		Result result = null;
		ResultSet results = session.execute(String.format("SELECT content FROM edm WHERE id = '%s'", id));
		for (Row row : results) {
			String jsonString = row.getString("content");
			calculator.measure(jsonString);

			result = new Result();
			result.setResults(calculator.getCounters().getResults());
			result.setExistingFields(calculator.getExistingFields());
			result.setMissingFields(calculator.getMissingFields());
			result.setEmptyFields(calculator.getEmptyFields());
			result.setTermsCollection(calculator.getTermsCollection());

			break;
		}

		ObjectMapper mapper = new ObjectMapper();
		String jsonInString = "";
		try {
			jsonInString = mapper.writeValueAsString(result);
		} catch (IOException ex) {
			LOGGER.severe(ex.getLocalizedMessage());
		}
		System.out.println(jsonInString);

		session.close();
		cluster.close();
	}
}
