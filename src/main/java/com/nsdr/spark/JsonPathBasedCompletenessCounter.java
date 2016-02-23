package com.nsdr.spark;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import net.minidev.json.JSONArray;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class JsonPathBasedCompletenessCounter implements Serializable {

	private String inputFileName;
	private String recordID;
	private String dataProvider;
	private Map<String, Integer> dataProviders;
	private Counters counters;
	private List<String> missingFields;
	private List<String> emptyFields;
	private List<String> existingFields;
	private boolean verbose = false;
	private static final String idPath = "$.identifier";
	private static final String dataProviderPath = "$.['ore:Aggregation'][0]['edm:dataProvider'][0]";

	public JsonPathBasedCompletenessCounter() {
		this.recordID = null;
	}

	public JsonPathBasedCompletenessCounter(String recordID) {
		this.recordID = recordID;
	}

	public void count(String jsonString) throws InvalidJsonException {
		Object document = Configuration.defaultConfiguration().jsonProvider().parse(jsonString);
		if (verbose) {
			missingFields = new ArrayList<>();
			emptyFields = new ArrayList<>();
			existingFields = new ArrayList<>();
		}
		setRecordID((String) JsonPath.read(document, idPath));
		setDataProvider(extractString(JsonPath.read(document, dataProviderPath)));
		counters = new Counters();
		for (JsonBranch jp : EdmBranches.getPaths()) {
			Object value = null;
			try {
				value = JsonPath.read(document, jp.getJsonPath());
			} catch (PathNotFoundException e) {
				// System.err.println("PathNotFoundException: " + e.getLocalizedMessage());
			}
			counters.increaseTotal(jp.getCategories());
			if (value != null) {
				if (value.getClass() == JSONArray.class) {
					if (!((JSONArray) value).isEmpty()) {
						counters.increaseInstance(jp.getCategories());
						if (verbose) {
							existingFields.add(jp.getLabel());
						}
					} else if (verbose) {
						missingFields.add(jp.getLabel());
					}
				} else if (value.getClass() == String.class) {
					if (StringUtils.isNotBlank((String) value)) {
						counters.increaseInstance(jp.getCategories());
						if (verbose) {
							existingFields.add(jp.getLabel());
						}
					} else if (verbose) {
						emptyFields.add(jp.getLabel());
					}
				} else {
					System.err.println(jp.getLabel() + " value.getClass(): " + value.getClass());
					System.err.println(jp.getLabel() + ": " + value);
				}
			} else if (verbose) {
				missingFields.add(jp.getLabel());
			}
		}
	}

	public String getFullResults(boolean withLabel) {
		return String.format("%s,%s,%s",
				  getDataProviderCode(), recordID, counters.getResultsAsCSV(withLabel));
	}

	public String getDataProviderCode() {
		String dataProviderCode;
		if (dataProvider == null) {
			dataProviderCode = "0";
		} else if (dataProviders != null && dataProviders.containsKey(dataProvider)) {
			dataProviderCode = String.valueOf(dataProviders.get(dataProvider));
		} else {
			dataProviderCode = dataProvider;
		}
		return dataProviderCode;
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

	public void setDataProviders(Map<String, Integer> dataProviders) {
		this.dataProviders = dataProviders;
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

	void setInputFileName(String inputFileName) {
		this.inputFileName = inputFileName;
	}

	public String getInputFileName() {
		return inputFileName;
	}

}
