package com.nsdr.spark;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.nsdr.spark.cli.Result;
import com.nsdr.spark.completeness.CompletenessCalculator;
import com.nsdr.spark.counters.Counters;
import com.nsdr.spark.uniqueness.TfIdfCalculator;
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

		final CompletenessCalculator completenessCalculator = new CompletenessCalculator();
		completenessCalculator.setVerbose(true);
		
		final TfIdfCalculator tfIdfCalculator = new TfIdfCalculator();
		tfIdfCalculator.setDoCollectTerms(true);
		// DataProviderManager dataProviderManager = new DataProviderManager();
		// counter.setDataProviderManager(dataProviderManager);
		// DatasetManager datasetManager = new DatasetManager();
		// counter.setDatasetManager(datasetManager);

		Result result = null;
		ResultSet results = session.execute(String.format("SELECT content FROM edm WHERE id = '%s'", id));
		for (Row row : results) {
			String jsonString = row.getString("content");
			Counters counters = new Counters();
			counters.doReturnFieldExistenceList(true);
			counters.doReturnTfIdfList(true);

			completenessCalculator.calculate(jsonString, counters);
			tfIdfCalculator.calculate(jsonString, counters);
			// result = counter.getFullResults(true, true);

			result = new Result();
			result.setResults(counters.getResults());
			result.setExistingFields(completenessCalculator.getExistingFields());
			result.setMissingFields(completenessCalculator.getMissingFields());
			result.setEmptyFields(completenessCalculator.getEmptyFields());
			result.setTermsCollection(tfIdfCalculator.getTermsCollection());

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
