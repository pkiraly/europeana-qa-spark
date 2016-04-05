package com.nsdr.spark.completeness;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.spi.json.JsonProvider;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import net.minidev.json.JSONArray;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class CompletenessCounter implements Serializable {

	private static final Logger logger = Logger.getLogger(CompletenessCounter.class.getCanonicalName());

	private String inputFileName;
	private String recordID;
	private String dataProvider;
	private String dataset;

	private DataProviderManager dataProviderManager;
	private DatasetManager datasetsManager;

	private Counters counters;
	private List<String> missingFields;
	private List<String> emptyFields;
	private List<String> existingFields;

	private boolean verbose = false;
	private boolean returnFieldExistenceList = false;
	private boolean returnTfIdf = false;

	private static final String idPath = "$.identifier";
	private static final String dataProviderPath = "$.['ore:Aggregation'][0]['edm:dataProvider'][0]";
	private static final String datasetPath = "$.sets[0]";
	private static List<FieldGroup> fieldGroups = new ArrayList<>();

	private static final String SOLR_SEARCH_PATH = "http://localhost:8983/solr/europeana/tvrh/?q=id:\"%s\""
			  + "&version=2.2&indent=on&qt=tvrh&tv=true&tv.all=true&f.includes.tv.tf=true"
			  + "&tv.fl=dc_title_txt,dc_description_txt,dcterms_alternative_txt"
			  + "&wt=json&json.nl=map&rows=1000&fl=id";
	private static final HttpClient httpClient = new HttpClient();
	private static final JsonProvider jsonProvider = Configuration.defaultConfiguration().jsonProvider();

	static {
		fieldGroups.add(new FieldGroup(JsonBranch.Category.MANDATORY, "Proxy/dc:title", "Proxy/dc:description"));
		fieldGroups.add(new FieldGroup(JsonBranch.Category.MANDATORY, "Proxy/dc:type", "Proxy/dc:subject", "Proxy/dc:coverage", "Proxy/dcterms:temporal", "Proxy/dcterms:spatial"));
		fieldGroups.add(new FieldGroup(JsonBranch.Category.MANDATORY, "Aggregation/edm:isShownAt", "Aggregation/edm:isShownBy"));

	}

	public CompletenessCounter() {
		this.recordID = null;
	}

	public CompletenessCounter(String recordID) {
		this.recordID = recordID;
	}

	public void count(String jsonString) throws InvalidJsonException {
		Object document = jsonProvider.parse(jsonString);
		if (verbose) {
			missingFields = new ArrayList<>();
			emptyFields = new ArrayList<>();
			existingFields = new ArrayList<>();
		}

		setRecordID((String) JsonPath.read(document, idPath));
		setDataProvider(extractString(JsonPath.read(document, dataProviderPath)));
		setDataset((String) JsonPath.read(document, datasetPath));

		counters = new Counters();
		for (JsonBranch jsonBranch : EdmBranches.getPaths()) {
			evaluateJsonBranch(jsonBranch, document);
		}

		for (FieldGroup fieldGroup : fieldGroups) {
			boolean existing = false;
			for (String field : fieldGroup.getFields()) {
				if (counters.getExistenceMap().get(field) == true) {
					existing = true;
					break;
				}
			}
			counters.increaseInstance(fieldGroup.getCategory(), existing);
		}

		counters.setTfIdfList(getTdIdf());
	}

	public void evaluateJsonBranch(JsonBranch jsonBranch, Object document) {
		Object value = null;
		try {
			if (jsonBranch.hasFilter()) {
				value = JsonPath.read(document, jsonBranch.getJsonPath(), jsonBranch.getFilter());
			} else {
				value = JsonPath.read(document, jsonBranch.getJsonPath());
			}
		} catch (PathNotFoundException e) {
			// System.err.println("PathNotFoundException: " + e.getLocalizedMessage());
		}
		counters.increaseTotal(jsonBranch.getCategories());
		if (value != null) {
			if (value.getClass() == JSONArray.class) {
				if (!((JSONArray) value).isEmpty()) {
					counters.increaseInstance(jsonBranch.getCategories());
					counters.addExistence(jsonBranch.getLabel(), true);
					if (verbose && !jsonBranch.hasFilter()) {
						existingFields.add(jsonBranch.getLabel());
					}
				} else if (!jsonBranch.hasFilter()) {
					counters.addExistence(jsonBranch.getLabel(), false);
					if (verbose) {
						missingFields.add(jsonBranch.getLabel());
					}
				}
			} else if (value.getClass() == String.class) {
				if (StringUtils.isNotBlank((String) value)) {
					counters.increaseInstance(jsonBranch.getCategories());
					counters.addExistence(jsonBranch.getLabel(), true);
					if (verbose && !jsonBranch.hasFilter()) {
						existingFields.add(jsonBranch.getLabel());
					}
				} else if (!jsonBranch.hasFilter()) {
					counters.addExistence(jsonBranch.getLabel(), false);
					if (verbose) {
						emptyFields.add(jsonBranch.getLabel());
					}
				}
			} else {
				System.err.println(jsonBranch.getLabel() + " value.getClass(): " + value.getClass());
				System.err.println(jsonBranch.getLabel() + ": " + value);
			}
		} else {
			counters.addExistence(jsonBranch.getLabel(), false);
			if (verbose) {
				missingFields.add(jsonBranch.getLabel());
			}
		}
	}

	public String getFullResults(boolean withLabel) {
		return getFullResults(withLabel, false);
	}

	public String getFullResults(boolean withLabel, boolean compress) {
		String result = String.format("%s,%s,%s,%s",
				  getDatasetCode(), getDataProviderCode(), recordID,
				  counters.getResultsAsCSV(withLabel, compress));
		if (returnFieldExistenceList == true) {
			result += ',' + counters.getExistenceList(withLabel);
		}
		if (returnTfIdf == true) {
			result += ',' + counters.getTfIdfList(withLabel);
		}
		return result;
	}

	public String getDataProviderCode() {
		String dataProviderCode;
		if (dataProvider == null) {
			dataProviderCode = "0";
		} else if (dataProviderManager != null) {
			dataProviderCode = String.valueOf(dataProviderManager.lookup(dataProvider));
		} else {
			dataProviderCode = dataProvider;
		}
		return dataProviderCode;
	}

	public String getDatasetCode() {
		String datasetCode;
		if (dataset == null) {
			datasetCode = "0";
		} else if (datasetsManager != null) {
			datasetCode = String.valueOf(datasetsManager.lookup(dataset));
		} else {
			datasetCode = dataset;
		}
		return datasetCode;
	}

	public Counters getCounters() {
		return counters;
	}

	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	public List<String> getMissingFields() {
		return missingFields;
	}

	public List<String> getEmptyFields() {
		return emptyFields;
	}

	public List<String> getExistingFields() {
		return existingFields;
	}

	public String getRecordID() {
		return recordID;
	}

	public void setRecordID(String recordID) {
		this.recordID = recordID;
	}

	public String getDataProvider() {
		return dataProvider;
	}

	public void setDataProvider(String dataProvider) {
		this.dataProvider = dataProvider;
	}

	public void setDataProviderManager(DataProviderManager dataProviderManager) {
		this.dataProviderManager = dataProviderManager;
	}

	public void setDatasetManager(DatasetManager datasetsManager) {
		this.datasetsManager = datasetsManager;
	}

	private String extractString(Object value) {
		String extracted = null;
		if (value.getClass() == String.class) {
			extracted = (String) value;
		} else if (value.getClass() == LinkedHashMap.class) {
			Map<String, String> map = (LinkedHashMap<String, String>) value;
			for (String val : map.values()) {
				extracted = val;
				break;
			}
		}
		return extracted;
	}

	public void setInputFileName(String inputFileName) {
		this.inputFileName = inputFileName;
	}

	public String getInputFileName() {
		return inputFileName;
	}

	public String getDataset() {
		return dataset;
	}

	public void setDataset(String dataset) {
		this.dataset = dataset;
	}

	public void doReturnFieldExistenceList(boolean returnFieldExistenceList) {
		this.returnFieldExistenceList = returnFieldExistenceList;
	}

	public void doReturnTfIdfList(boolean returnTfIdf) {
		this.returnTfIdf = returnTfIdf;
	}

	private Map<String, Double> getTdIdf() {
		Map<String, Double> tfIdfResult = null;
		String url = String.format(SOLR_SEARCH_PATH, getRecordID()).replace("\"", "%22");
		HttpMethod method = new GetMethod(url);
		try {
			int statusCode = httpClient.executeMethod(method);
			if (statusCode != HttpStatus.SC_OK) {
				logger.severe("Method failed: " + method.getStatusLine());
			}

			// Read the response body.
			byte[] responseBody = method.getResponseBody();
			String jsonString = new String(responseBody, Charset.forName("UTF-8"));
			TfIdfExtractor extractor = new TfIdfExtractor();
			tfIdfResult = extractor.extract(jsonString, getRecordID());
		} catch (HttpException e) {
			logger.severe("Fatal protocol violation: " + e.getMessage());
		} catch (IOException e) {
			logger.severe("Fatal transport error: " + e.getMessage());
		} finally {
			method.releaseConnection();
		}

		return tfIdfResult;
	}

}
