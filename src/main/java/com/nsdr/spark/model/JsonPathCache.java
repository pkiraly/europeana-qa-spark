package com.nsdr.spark.model;

import com.jayway.jsonpath.JsonPath;
import com.nsdr.spark.util.JsonUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class JsonPathCache {

	Map<String, List<EdmFieldInstance>> cache = new HashMap<>();

	public void set(Object jsonDocument, String jsonPath) {
		Object value = JsonPath.read(jsonDocument, jsonPath);
		List<EdmFieldInstance> instances = null;
		if (value != null) {
			instances = JsonUtils.extractFieldInstanceList(value);
		}
		cache.put(jsonPath, instances);
	}

	public List<EdmFieldInstance> get(Object jsonDocument, String jsonPath) {
		if (!cache.containsKey(jsonPath)) {
			set(jsonDocument, jsonPath);
		}
		return cache.get(jsonPath);
	}
}
