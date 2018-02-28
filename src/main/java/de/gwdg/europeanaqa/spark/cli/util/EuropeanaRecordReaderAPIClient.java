package de.gwdg.europeanaqa.spark.cli.util;

import javax.ws.rs.client.*;
import javax.ws.rs.core.MediaType;
import java.io.Serializable;

public class EuropeanaRecordReaderAPIClient implements Serializable {

	private static final String REST_URI = "http://144.76.218.178:8080/europeana-qa/record/%s.json";
	private Client client = ClientBuilder.newClient();


	public String getRecord(String recordId) {
		String json = client
			.target(String.format(REST_URI, recordId))
			.queryParam("dataSource", "cassandra")
			.request(MediaType.APPLICATION_JSON)
			.get(String.class);

		return json;
	}
}
