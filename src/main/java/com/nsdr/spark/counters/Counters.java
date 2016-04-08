package com.nsdr.spark.counters;

import com.nsdr.spark.completeness.JsonBranch;
import com.nsdr.spark.counters.BasicCounter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class Counters {

	private static final String TOTAL = "TOTAL";
	private String recordId;
	private Map<String, Object> fields = new LinkedHashMap<>();
	private Map<String, BasicCounter> basicCounters;
	private final Map<String, Boolean> existenceList = new LinkedHashMap<>();
	private Map<String, Double> tfIdfList;

	private boolean returnFieldExistenceList = false;
	private boolean returnTfIdf = false;

	public Counters() {
		initialize();
	}

	public void calculateResults() {
		for (BasicCounter counter : basicCounters.values()) {
			counter.calculate();
		}
	}

	public Map<String, Double> getResults() {
		calculateResults();
		Map<String, Double> result = new HashMap<>();
		for (Map.Entry<String, BasicCounter> entry : basicCounters.entrySet()) {
			result.put(entry.getKey(), entry.getValue().getResult());
		}

		if (returnTfIdf == true) {
			result.putAll(tfIdfList);
		}

		return result;
	}

	public List<String> getResultsAsList() {
		return getResultsAsList(true);
	}

	public List<String> getResultsAsList(boolean withLabel) {
		return getResultsAsList(withLabel, false);
	}

	public List<String> getResultsAsList(boolean withLabel, boolean compressed) {
		Map<String, Double> results = getResults();
		List<String> items = new ArrayList<>();
		addResultItem(withLabel, items, TOTAL, results.get(TOTAL), compressed);
		for (JsonBranch.Category category : JsonBranch.Category.values()) {
			addResultItem(withLabel, items, category.name(), results.get(category.name()), compressed);
		}
		return items;
	}

	private void addResultItem(boolean withLabel, List<String> items, String key, Double value, boolean compressed) {
		String valueAsString = String.format("%f", value);
		if (compressed) {
			valueAsString = compressNumber(valueAsString);
		}

		if (withLabel) {
			items.add(String.format("\"%s\":%s", key, valueAsString));
		} else {
			items.add(valueAsString);
		}
	}

	/**
	 * Removes the unnecessary 0-s from the end of a number
	 * For example 0.7000 becomes 0.7, 0.00000 becomes 0.0
	 * @param value A string representation of a number
	 * @return The "compressed" representation without zeros at the end
	 */
	public static String compressNumber(String value) {
		return value.replaceAll("([0-9])0+$", "$1").replaceAll("\\.0+$", ".0");
	}

	public String getResultsAsTSV(boolean withLabel) {
		return StringUtils.join(getResultsAsList(withLabel), "\t");
	}

	public String getResultsAsCSV(boolean withLabel) {
		return getResultsAsCSV(withLabel, false);
	}

	public String getResultsAsCSV(boolean withLabel, boolean compressed) {
		return StringUtils.join(getResultsAsList(withLabel, compressed), ",");
	}

	public void printResults() {
		calculateResults();
		for (Map.Entry<String, BasicCounter> entry : basicCounters.entrySet()) {
			System.err.println(entry.getKey() + ": " + entry.getValue().getResult());
		}
	}

	public void increaseInstance(List<JsonBranch.Category> categories) {
		basicCounters.get(TOTAL).increaseInstance();
		for (JsonBranch.Category category : categories) {
			basicCounters.get(category.name()).increaseInstance();
		}
	}

	public void increaseInstance(JsonBranch.Category category, boolean increase) {
		basicCounters.get(category.name()).increaseTotal();
		if (increase) {
			basicCounters.get(category.name()).increaseInstance();
		}
	}

	public void increaseTotal(List<JsonBranch.Category> categories) {
		basicCounters.get(TOTAL).increaseTotal();
		for (JsonBranch.Category category : categories) {
			basicCounters.get(category.name()).increaseTotal();
		}
	}

	private void initialize() {
		basicCounters = new HashMap<>();
		basicCounters.put(TOTAL, new BasicCounter());
		for (JsonBranch.Category category : JsonBranch.Category.values()) {
			basicCounters.put(category.name(), new BasicCounter());
		}
	}

	public BasicCounter getStatComponent(JsonBranch.Category category) {
		return basicCounters.get(category.name());
	}

	public void addExistence(String fieldName, Boolean existence) {
		existenceList.put(fieldName, existence);
	}

	public Map<String, Boolean> getExistenceMap() {
		return existenceList;
	}

	public String getExistenceList(boolean withLabel) {
		List<String> items = new ArrayList<>();
		for (Map.Entry<String, Boolean> entry : existenceList.entrySet()) {
			String item = "";
			if (withLabel) {
				item += String.format("\"%s\":", entry.getKey());
			}
			item += (entry.getValue() == true) ? "1" : "0";
			items.add(item);
		}
		return StringUtils.join(items, ',');
	}

	public List<Integer> getExistenceList() {
		List<Integer> values = new LinkedList<>();
		for (boolean val : existenceList.values()) {
			values.add((val) ? 1 : 0);
		}
		return values;
	}

	public void setTfIdfList(Map<String, Double> tdIdf) {
		this.tfIdfList = tdIdf;
	}

	public String getTfIdfList(boolean withLabel) {
		List<String> items = new ArrayList<>();
		for (Map.Entry<String, Double> entry : tfIdfList.entrySet()) {
			String item = "";
			if (withLabel) {
				item += String.format("\"%s\":", entry.getKey());
			}
			item += String.format("%.8f", entry.getValue());
			items.add(item);
		}
		return StringUtils.join(items, ',');
	}

	public String getRecordId() {
		return recordId;
	}

	public void setRecordId(String recordId) {
		this.recordId = recordId;
	}

	public void setField(String key, Object value) {
		fields.put(key, value);
	}

	public Object getField(String key) {
		return fields.get(key);
	}

	public String getFullResults(boolean withLabel) {
		return getFullResults(withLabel, false);
	}

	public String getFullResults(boolean withLabel, boolean compress) {
		String result = String.format("%s,%s,%s,%s",
				fields.get("datasetCode"), fields.get("dataProviderCode"), recordId,
				getResultsAsCSV(withLabel, compress));
		if (returnFieldExistenceList == true) {
			result += ',' + getExistenceList(withLabel);
		}
		if (returnTfIdf == true) {
			result += ',' + getTfIdfList(withLabel);
		}
		return result;
	}

	public void doReturnFieldExistenceList(boolean returnFieldExistenceList) {
		this.returnFieldExistenceList = returnFieldExistenceList;
	}

	public void doReturnTfIdfList(boolean returnTfIdf) {
		this.returnTfIdf = returnTfIdf;
	}

}
