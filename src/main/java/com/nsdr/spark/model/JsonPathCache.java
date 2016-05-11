package com.nsdr.spark.model;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.nsdr.spark.util.JsonUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class JsonPathCache {

	private final Object jsonDocument;
	private final Map<String, List<EdmFieldInstance>> cache = new HashMap<>();
	private String recordId;

	public JsonPathCache(String jsonString) throws InvalidJsonException {
		this.jsonDocument = Configuration.defaultConfiguration().jsonProvider().parse(jsonString);
	}

	public JsonPathCache(Object jsonDocument) {
		this.jsonDocument = jsonDocument;
	}

	private void set(String jsonPath) {
		List<EdmFieldInstance> instances = null;
		try {
			Object value = JsonPath.read(jsonDocument, jsonPath);
			if (value != null) {
				instances = JsonUtils.extractFieldInstanceList(value, recordId);
			}
		} catch (PathNotFoundException e) {
			//
		}
		cache.put(jsonPath, instances);
	}

	public List<EdmFieldInstance> get(String jsonPath) {
		if (!cache.containsKey(jsonPath)) {
			set(jsonPath);
		}
		return cache.get(jsonPath);
	}

	public String getRecordId() {
		return recordId;
	}

	public void setRecordId(String recordId) {
		this.recordId = recordId;
	}
}
