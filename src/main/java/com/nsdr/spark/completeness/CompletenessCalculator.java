package com.nsdr.spark.completeness;

import com.nsdr.spark.counters.Counters;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.nsdr.spark.interfaces.Calculator;
import com.nsdr.spark.model.EdmFieldInstance;
import com.nsdr.spark.model.JsonPathCache;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class CompletenessCalculator implements Calculator, Serializable {

	private static final Logger LOGGER = Logger.getLogger(CompletenessCalculator.class.getCanonicalName());

	private String inputFileName;

	private DataProviderManager dataProviderManager;
	private DatasetManager datasetsManager;

	// private Counters counters;
	private List<String> missingFields;
	private List<String> emptyFields;
	private List<String> existingFields;

	private boolean verbose = false;

	private static final String ID_PATH = "$.identifier";
	private static final String DATA_PROVIDER_PATH = "$.['ore:Aggregation'][0]['edm:dataProvider'][0]";
	private static final String DATASET_PATH = "$.sets[0]";
	private static final List<FieldGroup> FIELD_GROUPS = new ArrayList<>();
	private static final JsonProvider JSON_PROVIDER = Configuration.defaultConfiguration().jsonProvider();

	static {
		FIELD_GROUPS.add(new FieldGroup(JsonBranch.Category.MANDATORY, "Proxy/dc:title", "Proxy/dc:description"));
		FIELD_GROUPS.add(new FieldGroup(JsonBranch.Category.MANDATORY, "Proxy/dc:type", "Proxy/dc:subject", "Proxy/dc:coverage", "Proxy/dcterms:temporal", "Proxy/dcterms:spatial"));
		FIELD_GROUPS.add(new FieldGroup(JsonBranch.Category.MANDATORY, "Aggregation/edm:isShownAt", "Aggregation/edm:isShownBy"));
	}

	public CompletenessCalculator() {
		// this.recordID = null;
	}

	public CompletenessCalculator(String recordID) {
		// this.recordID = recordID;
	}

	@Override
	public void calculate(JsonPathCache cache, Counters counters) throws InvalidJsonException {
		// Object document = JSON_PROVIDER.parse(jsonString);
		if (verbose) {
			missingFields = new ArrayList<>();
			emptyFields = new ArrayList<>();
			existingFields = new ArrayList<>();
		}

		counters.setRecordId(cache.get(ID_PATH).get(0).getValue());
		cache.setRecordId(counters.getRecordId());
		List<EdmFieldInstance> providers = cache.get(DATA_PROVIDER_PATH);
		// if (providers)
		counters.setField("dataProvider", cache.get(DATA_PROVIDER_PATH).get(0).getValue());
		counters.setField("dataProviderCode", getDataProviderCode(cache.get(DATA_PROVIDER_PATH).get(0).getValue()));
		counters.setField("dataset", cache.get(DATASET_PATH).get(0).getValue());
		counters.setField("datasetCode", getDatasetCode(cache.get(DATASET_PATH).get(0).getValue()));

		for (JsonBranch jsonBranch : EdmBranches.getPaths()) {
			evaluateJsonBranch(jsonBranch, cache, counters);
		}

		for (FieldGroup fieldGroup : FIELD_GROUPS) {
			boolean existing = false;
			for (String field : fieldGroup.getFields()) {
				if (counters.getExistenceMap().get(field) == true) {
					existing = true;
					break;
				}
			}
			counters.increaseInstance(fieldGroup.getCategory(), existing);
		}
	}

	public void evaluateJsonBranch(JsonBranch jsonBranch, JsonPathCache cache, Counters counters) {
		List<EdmFieldInstance> values = cache.get(jsonBranch.getJsonPath());
		counters.increaseTotal(jsonBranch.getCategories());
		if (values != null && !values.isEmpty()) {
			counters.increaseInstance(jsonBranch.getCategories());
			counters.addExistence(jsonBranch.getLabel(), true);
			counters.addInstance(jsonBranch.getLabel(), values.size());
			if (verbose) {
				existingFields.add(jsonBranch.getLabel());
			}
		} else {
			counters.addExistence(jsonBranch.getLabel(), false);
			counters.addInstance(jsonBranch.getLabel(), 0);
			if (verbose) {
				missingFields.add(jsonBranch.getLabel());
			}
		}
	}

	public String getDataProviderCode(String dataProvider) {
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

	public String getDatasetCode(String dataset) {
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

}
