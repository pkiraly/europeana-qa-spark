package com.nsdr.spark.util;

import com.jayway.jsonpath.JsonPath;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class JsonUtils {

	public static Object extractField(Object document, String jsonPath) {
		return JsonPath.read(document, jsonPath);
	}

	public static List<String> extractList(Object value) {
		List<String> extracted = new ArrayList<>();
		if (value.getClass() == String.class) {
			extracted.add((String) value);
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
		}
		return extracted;
	}

}
