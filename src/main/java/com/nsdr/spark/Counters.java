package com.nsdr.spark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class Counters {

	private static final String TOTAL = "TOTAL";
	private Map<String, BasicCounter> basicCounters;

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
}
