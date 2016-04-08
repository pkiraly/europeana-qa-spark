package com.nsdr.spark.uniqueness;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.spi.json.JsonProvider;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class TfIdfExtractor {

	private static final JsonProvider jsonProvider = Configuration.defaultConfiguration().jsonProvider();
	private static final Map<String, String> termFields = new LinkedHashMap<>();
	static {
		termFields.put("dc:title", "dc_title_txt");
		termFields.put("dcterms:alternative", "dcterms_alternative_txt");
		termFields.put("dc:description", "dc_description_txt");
	}

	private Map<String, List<TfIdf>> termsCollection;

	public Map<String, Double> extract(String jsonString, String recordId) {
		return extract(jsonString, recordId, false);
	}

	public Map<String, Double> extract(String jsonString, String recordId, boolean doCollectTerms) {
		Map<String, Double> results = new LinkedHashMap<>();
		termsCollection = new LinkedHashMap<>();
		Object document = jsonProvider.parse(jsonString);
		String path = String.format("$.termVectors.['%s']", recordId);
		Map value = (LinkedHashMap) JsonPath.read(document, path);
		for (String field : termFields.keySet()) {
			termsCollection.put(field, new LinkedList<TfIdf>());
			String solrField = termFields.get(field);
			double sum = 0;
			double count = 0;
			if (value.containsKey(solrField)) {
				Map terms = (LinkedHashMap) value.get(solrField);
				for (String term : (Set<String>) terms.keySet()) {
					Map termInfo = (LinkedHashMap) terms.get(term);
					int tf = getInt(termInfo.get("tf"));
					int df = getInt(termInfo.get("df"));
					double tfIdf = getDouble(termInfo.get("tf-idf"));
					termsCollection.get(field).add(new TfIdf(term, tf, df, tfIdf));
					sum += tfIdf;
					count++;
				}
			}
			double avg = count > 0 ? sum / count : 0;
			results.put(field + ":sum", sum);
			results.put(field + ":avg", avg);
		}
		return results;
	}

	public Map<String, List<TfIdf>> getTermsCollection() {
		return termsCollection;
	}

	public Double getDouble(Object value) {
		double doubleValue;
		if (value.getClass().getCanonicalName().equals("java.math.BigDecimal")) {
			doubleValue = ((BigDecimal) value).doubleValue();
		} else if (value.getClass().getCanonicalName().equals("java.lang.Integer")) {
			doubleValue = ((Integer) value).doubleValue();
		} else {
			doubleValue = (Double) value;
		}
		return doubleValue;
	}

	public Integer getInt(Object value) {
		int intValue;
		if (value.getClass().getCanonicalName().equals("java.math.BigDecimal")) {
			intValue = ((BigDecimal) value).intValue();
		} else if (value.getClass().getCanonicalName().equals("java.lang.Integer")) {
			intValue = (Integer) value;
		} else {
			intValue = (Integer) value;
		}
		return intValue;
	}
}
