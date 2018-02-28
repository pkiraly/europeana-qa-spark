package de.gwdg.europeanaqa.spark.cli.util;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import javax.ws.rs.client.*;
import javax.ws.rs.core.MediaType;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;

public class EuropeanaRecordReaderAPIClient implements Serializable {

	private static final String REST_URI = "http://144.76.218.178:8080/europeana-qa/record/%s.json";
	private static final String REST_URI_WITH_PARAMS = "http://144.76.218.178:8080/europeana-qa/record/%s.json?dataSource=mongo";
	private Client client = ClientBuilder.newClient();

	private final String USER_AGENT = "Custom Java application";


	public static void main(String[] args) throws Exception {
		EuropeanaRecordReaderAPIClient http = new EuropeanaRecordReaderAPIClient();
		System.out.println("Testing 1 - Send Http GET request");
		http.getRecord("91001/006ED5FBDCEF5B1847C443CA4829A65ABE55A917");
	}

	public String getRecordWithJersey(String recordId) {
		String json = client
			.target(String.format(REST_URI, recordId))
			.queryParam("dataSource", "cassandra")
			.request(MediaType.APPLICATION_JSON)
			.get(String.class);

		return json;
	}

	// HTTP GET request
	public String getRecord(String recordId) throws Exception {

		String url = String.format(REST_URI_WITH_PARAMS, recordId);

		HttpClient client = new DefaultHttpClient();
		HttpGet request = new HttpGet(url);
		request.addHeader("User-Agent", USER_AGENT);

		HttpResponse response = client.execute(request);
		//System.out.println("Response Code : " + response.getStatusLine().getStatusCode());

		BufferedReader rd = new BufferedReader(
			new InputStreamReader(
				response.getEntity().getContent()));

		StringBuffer result = new StringBuffer();
		String line = "";
		while ((line = rd.readLine()) != null) {
			result.append(line);
		}

		return result.toString();
	}
}
