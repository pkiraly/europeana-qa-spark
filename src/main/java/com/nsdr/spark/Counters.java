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

	private Map<String, double[]> stat;

	public Counters() {
		initialize();
	}

	public void calculateResults() {
		for (String key : stat.keySet()) {
			if (stat.get(key)[1] != 0.0) {
				stat.get(key)[2] = (stat.get(key)[1] / stat.get(key)[0]);
			}
		}
	}

	public Map<String, Double> getResults() {
		calculateResults();
		Map<String, Double> result = new HashMap<>();
		for (String key : stat.keySet()) {
			result.put(key, stat.get(key)[2]);
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
			valueAsString = valueAsString.replaceAll("([0-9])0+$", "$1").replaceAll("\\.0+$", ".0");
		}

		if (withLabel) {
			items.add(String.format("\"%s\":%s", key, valueAsString));
		} else {
			items.add(valueAsString);
		}
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
		for (String key : stat.keySet()) {
			System.err.println(key + ": " + stat.get(key)[2]);
		}
	}

	public void increaseInstance(List<JsonBranch.Category> categories) {
		stat.get(TOTAL)[1]++;
		for (JsonBranch.Category category : categories) {
			stat.get(category.name())[1]++;
		}
	}

	public void increaseTotal(List<JsonBranch.Category> categories) {
		stat.get(TOTAL)[0]++;
		for (JsonBranch.Category category : categories) {
			stat.get(category.name())[0]++;
		}
	}

	private void initialize() {
		stat = new HashMap<>();
		stat.put(TOTAL, new double[]{0.0, 0.0, 0.0});
		for (JsonBranch.Category category : JsonBranch.Category.values()) {
			stat.put(category.name(), new double[]{0.0, 0.0, 0.0});
		}
	}

}
