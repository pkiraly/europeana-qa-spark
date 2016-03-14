package com.nsdr.spark.cli;

import java.util.List;
import java.util.Map;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class Result {
	private List<String> existingFields;
	private List<String> missingFields;
	private List<String> emptyFields;
	private Map<String, Double> results;

	public Result() {
	}

	public List<String> getExistingFields() {
		return existingFields;
	}

	public void setExistingFields(List<String> existingFields) {
		this.existingFields = existingFields;
	}

	public List<String> getMissingFields() {
		return missingFields;
	}

	public void setMissingFields(List<String> missingFields) {
		this.missingFields = missingFields;
	}

	public List<String> getEmptyFields() {
		return emptyFields;
	}

	public void setEmptyFields(List<String> emptyFields) {
		this.emptyFields = emptyFields;
	}

	public Map<String, Double> getResults() {
		return results;
	}

	public void setResults(Map<String, Double> results) {
		this.results = results;
	}

	
}
