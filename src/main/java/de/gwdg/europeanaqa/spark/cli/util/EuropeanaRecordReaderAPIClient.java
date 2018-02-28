package de.gwdg.europeanaqa.spark.cli.util;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;

public class EuropeanaRecordReaderAPIClient implements Serializable {

	private static final String REST_URI = "http://%s/europeana-qa/record/%s.json?dataSource=mongo";

	private final String USER_AGENT = "Custom Java application";
	private String host;

	public EuropeanaRecordReaderAPIClient(String host) {
		this.host = host;
	}

	public String getRecord(String recordId) throws Exception {

		String url = String.format(REST_URI, host, recordId);

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
