package com.nsdr.spark.completeness;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.spi.json.JsonProvider;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class TfIdfExtractor {

	private static final Map<String, String> termFields = new LinkedHashMap<>();
	private static final JsonProvider jsonProvider = Configuration.defaultConfiguration().jsonProvider();
	static {
		termFields.put("dc:title", "dc_title_txt");
		termFields.put("dcterms:alternative", "dcterms_alternative_txt");
		termFields.put("dc:description", "dc_description_txt");
	}

	public Map<String, Double> extract(String jsonString, String recordId) {
		Map<String, Double> results = new LinkedHashMap<>();
		Object document = jsonProvider.parse(jsonString);
		String path = String.format("$.termVectors.['%s']", recordId);
		Map value = (LinkedHashMap) JsonPath.read(document, path);
		for (String field : termFields.keySet()) {
			String solrField = termFields.get(field);
			double sum = 0;
			double count = 0;
			if (value.containsKey(solrField)) {
				Map terms = (LinkedHashMap) value.get(solrField);
				for (String term : (Set<String>) terms.keySet()) {
					Map termInfo = (LinkedHashMap) terms.get(term);
					BigDecimal tfIdf = (BigDecimal) termInfo.get("tf-idf");
					sum += tfIdf.doubleValue();
					count++;
				}
			}
			double avg = count > 0 ? sum / count : 0;
			results.put(field + ":sum", sum);
			results.put(field + ":avg", avg);
		}
		return results;
	}
}
