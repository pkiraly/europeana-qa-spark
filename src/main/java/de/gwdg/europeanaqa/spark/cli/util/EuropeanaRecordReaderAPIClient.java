package de.gwdg.europeanaqa.spark.cli.util;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import javax.net.ssl.HttpsURLConnection;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

public class EuropeanaRecordReaderAPIClient implements Serializable {

	private static final String GET_RECORD_URI = "http://%s/europeana-qa/record/%s.json?dataSource=mongo&batchMode=true";
	private static final String RESOLVE_FRAGMENT_URI = "http://%s/europeana-qa/resolve-json-fragment";
	private static final String RESOLVE_FRAGMENT_PARAMETERS = "batchMode=true&jsonFragment=%s";

	private final String USER_AGENT = "Custom Java application";
	private String host;

	public EuropeanaRecordReaderAPIClient(String host) {
		this.host = host;
	}

	public String getRecord(String recordId) throws Exception {

		String url = getRecordUrl(recordId);

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

	private String getRecordUrl(String recordId) {
		return String.format(GET_RECORD_URI, host, recordId);
	}

	private String getFragmentUrl() {
		return String.format(RESOLVE_FRAGMENT_URI, host);
	}

	private String getFragmentParameters(String jsonFragment) {
		return String.format(RESOLVE_FRAGMENT_PARAMETERS, jsonFragment);
	}

	public String getRecord2(String recordId) {
		URL url = null;
		HttpURLConnection urlConnection = null;
		String record = null;
		try {
			url = new URL(getRecordUrl(recordId));
			urlConnection = (HttpURLConnection) url.openConnection();
			InputStream in = new BufferedInputStream(urlConnection.getInputStream());
			record = readStream(in);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (urlConnection != null)
				urlConnection.disconnect();
		}
		return record;
	}

	public String resolveFragment(String jsonFragment) {
		URL url = null;
		HttpURLConnection urlConnection = null;
		String record = null;
		try {
			url = new URL(getFragmentUrl() + "?" + getFragmentParameters(jsonFragment));
			urlConnection = (HttpURLConnection) url.openConnection();
			InputStream in = new BufferedInputStream(urlConnection.getInputStream());
			record = readStream(in);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (urlConnection != null)
				urlConnection.disconnect();
		}
		return record;
	}

	// HTTP POST request
	private String resolveFragmentWithPost(String jsonFragment) throws Exception {
		URL url = new URL(getFragmentUrl());
		HttpsURLConnection urlConnection = (HttpsURLConnection) url.openConnection();

		//add reuqest header
		urlConnection.setRequestMethod("POST");
		urlConnection.setRequestProperty("User-Agent", USER_AGENT);
		urlConnection.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
		urlConnection.setDoOutput(true);
		DataOutputStream wr = new DataOutputStream(urlConnection.getOutputStream());
		wr.writeBytes(getFragmentParameters(jsonFragment));
		wr.flush();
		wr.close();

		String record = null;
		try {
			InputStream in = new BufferedInputStream(urlConnection.getInputStream());
			record = readStream(in);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (urlConnection != null)
				urlConnection.disconnect();
		}
		return record;
	}


	private String readStream(InputStream in) throws IOException {
		BufferedReader rd = new BufferedReader(new InputStreamReader(in));

		StringBuffer result = new StringBuffer();
		String line = "";
		while ((line = rd.readLine()) != null) {
			result.append(line);
		}

		return result.toString();

	}
}
