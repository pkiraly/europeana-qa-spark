package com.nsdr.spark;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.nsdr.spark.cli.Result;
import com.nsdr.spark.completeness.CompletenessCounter;
import com.nsdr.spark.completeness.DataProviderManager;
import com.nsdr.spark.completeness.DatasetManager;
import java.io.FileNotFoundException;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class CLI {

	private static Cluster cluster;
	private static Session session;

	public static void main(String[] args) throws FileNotFoundException {

		if (args.length < 1) {
			System.err.println("Please provide a record identifier");
			System.exit(0);
		}
		String id = args[0];

		cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		session = cluster.connect("europeana");

		final CompletenessCounter counter = new CompletenessCounter();
		// DataProviderManager dataProviderManager = new DataProviderManager();
		// counter.setDataProviderManager(dataProviderManager);
		// DatasetManager datasetManager = new DatasetManager();
		// counter.setDatasetManager(datasetManager);

		Result result = null;
		ResultSet results = session.execute(String.format("SELECT content FROM edm WHERE id = '%s'", id));
		for (Row row : results) {
			String jsonString = row.getString("content");
			counter.count(jsonString);
			counter.setVerbose(true);
			// result = counter.getFullResults(true, true);

			result = new Result();
			result.setResults(counter.getCounters().getResults());
			result.setExistingFields(counter.getExistingFields());
			result.setMissingFields(counter.getMissingFields());
			result.setEmptyFields(counter.getEmptyFields());

			break;
		}

		System.out.println(result);
	}
}
