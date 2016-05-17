package com.nsdr.spark.completeness;

import com.nsdr.spark.counters.Counters;
import com.jayway.jsonpath.InvalidJsonException;
import com.nsdr.spark.counters.BasicCounter;
import com.nsdr.spark.interfaces.Calculator;
import com.nsdr.spark.model.EdmFieldInstance;
import com.nsdr.spark.model.JsonPathCache;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class LanguageCalculator implements Calculator, Serializable {

	private static final Logger LOGGER = Logger.getLogger(LanguageCalculator.class.getCanonicalName());

	private String inputFileName;

	private DataProviderManager dataProviderManager;
	private DatasetManager datasetsManager;

	private Counters counters;
	private Map<String, String> languageMap;

	private boolean verbose = false;

	private static final String ID_PATH = "$.identifier";
	private static final String DATA_PROVIDER_PATH = "$.['ore:Aggregation'][0]['edm:dataProvider'][0]";
	private static final String DATASET_PATH = "$.sets[0]";

	private static final List<String> skipFields = Arrays.asList(
			"edm:ProvidedCHO/@about", "Proxy/edm:isNextInSequence",
			"Proxy/edm:type", "Aggregation/edm:isShownAt",
			"Aggregation/edm:isShownBy", "Aggregation/edm:object",
			"Aggregation/edm:hasView");

	public LanguageCalculator() {
		// this.recordID = null;
	}

	public LanguageCalculator(String recordID) {
		// this.recordID = recordID;
	}

	@Override
	public void calculate(JsonPathCache cache, Counters counters) throws InvalidJsonException {
		this.counters = counters;

		counters.setRecordId(cache.get(ID_PATH).get(0).getValue());
		cache.setRecordId(counters.getRecordId());

		setDatasetAndProvider(cache, counters);

		languageMap = new LinkedHashMap<>();
		for (JsonBranch jsonBranch : EdmBranches.getPaths()) {
			if (!skipFields.contains(jsonBranch.getLabel()))
				extractLanguageTags(jsonBranch, cache, languageMap);
		}
	}

	private void setDatasetAndProvider(JsonPathCache cache, Counters counters1) {
		String dataProvider = cache.get(DATA_PROVIDER_PATH).get(0).getValue();
		counters1.setField("dataProvider", dataProvider);
		counters1.setField("dataProviderCode", getDataProviderCode(dataProvider));
		String dataset = cache.get(DATASET_PATH).get(0).getValue();
		counters1.setField("dataset", dataset);
		counters1.setField("datasetCode", getDatasetCode(dataset));
	}

	public String getResult() {
		String result = String.format("%s,%s,%s,%s",
			counters.getField("datasetCode"),
			counters.getField("dataProviderCode"),
			counters.getRecordId(),
			StringUtils.join(languageMap.values(), ",")
		);
		return result;
	}

	private void extractLanguageTags(JsonBranch jsonBranch, JsonPathCache cache,
			Map<String, String> languageMap) {
		List<EdmFieldInstance> values = cache.get(jsonBranch.getJsonPath());
		Map<String, BasicCounter> languages = new HashMap<>();
		if (values != null && !values.isEmpty()) {
			for (EdmFieldInstance field : values) {
				if (field.hasValue()) {
					if (field.hasLanguage()) {
						if (!languages.containsKey(field.getLanguage())) {
							languages.put(field.getLanguage(), new BasicCounter(1));
						} else {
							languages.get(field.getLanguage()).increaseTotal();
						}
					} else {
						languages.put("_0", new BasicCounter(1));
					}
				}
			}
		} else {
			languages.put("_1", new BasicCounter(1));
		}
		languageMap.put(jsonBranch.getLabel(), extractLanguages(languages));
	}

	private String extractLanguages(Map<String, BasicCounter> languages) {
		String result = "";
		for (String lang : languages.keySet()) {
			if (result.length() > 0)
				result += ";";
			result += lang + ":" + ((Double)languages.get(lang).getTotal()).intValue();
		}
		return result;
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

	public void setDataProviderManager(DataProviderManager dataProviderManager) {
		this.dataProviderManager = dataProviderManager;
	}

	public void setDatasetManager(DatasetManager datasetsManager) {
		this.datasetsManager = datasetsManager;
	}

}
