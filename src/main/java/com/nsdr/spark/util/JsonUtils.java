package com.nsdr.spark.util;

import com.jayway.jsonpath.JsonPath;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import net.minidev.json.JSONArray;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class JsonUtils {

	private static final Logger logger = Logger.getLogger(JsonUtils.class.getCanonicalName());

	public static Object extractField(Object document, String jsonPath) {
		return JsonPath.read(document, jsonPath);
	}

	public static List<String> extractList(Object value) {
		List<String> extracted = new ArrayList<>();
		if (value.getClass() == String.class) {
			extracted.add((String) value);
		} else if (value.getClass() == JSONArray.class) {
			JSONArray array1 = (JSONArray) value;
			for (int i = 0, l = array1.size(); i < l; i++) {
				if (array1.get(i).getClass() == JSONArray.class) {
					JSONArray array2 = (JSONArray) array1.get(i);
					for (int j = 0, l2 = array2.size(); j < l2; j++) {
						if (array2.get(j).getClass() == String.class) {
							extracted.add((String) array2.get(j));
						} else if (array2.get(j).getClass() == LinkedHashMap.class) {
							Map<String, String> map = (LinkedHashMap<String, String>) array2.get(j);
							if (map.containsKey("@resource")) {
								extracted.add(map.get("@resource"));
							} else if (map.containsKey("#value")) {
								extracted.add(map.get("#value"));
							} else {
								logger.severe("Other type of map: " + map);
							}
						} else {
							logger.severe("unhandled array2 type: " + getType(array2.get(j)));
						}
					}
				} else {
					logger.severe("unhandled array1 type: " + getType(array1.get(i)));
				}
			}
		} else {
			logger.severe("unhandled object type: " + getType(value));
		}
		return extracted;
	}

	public static String extractString(Object value) {
		String extracted = null;
		if (value.getClass() == String.class) {
			extracted = (String) value;
		} else if (value.getClass() == LinkedHashMap.class) {
			Map<String, String> map = (LinkedHashMap<String, String>) value;
			for (String val : map.values()) {
				extracted = val;
				break;
			}
		} else {
			logger.severe("unhandled object type: " + getType(value));
		}
		return extracted;
	}

	private static String getType(Object obj) {
		return obj.getClass().getCanonicalName();
	}
}
