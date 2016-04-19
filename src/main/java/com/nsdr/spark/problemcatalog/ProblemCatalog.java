package com.nsdr.spark.problemcatalog;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.nsdr.spark.counters.Counters;
import com.nsdr.spark.interfaces.Calculator;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class ProblemCatalog implements Calculator, Serializable {

	private static final JsonProvider JSON_PROVIDER = Configuration.defaultConfiguration().jsonProvider();
	private static final Logger logger = Logger.getLogger(ProblemCatalog.class.getCanonicalName());

	private List<ProblemDetector> problems = new ArrayList<>();
	private String jsonString;
	private Object jsonDocument;
	private Map<String, Double> results;

	public String getJsonString() {
		return jsonString;
	}

	public Object getJsonDocument() {
		return jsonDocument;
	}

	public void attach(ProblemDetector observer) {
		problems.add(observer);
	}

	public void notifyAllObservers() {
		for (ProblemDetector observer : problems) {
			observer.update(jsonDocument, results);
		}
	}

	@Override
	public void calculate(String jsonString, Counters counters) {
		this.jsonString = jsonString;
		this.jsonDocument = JSON_PROVIDER.parse(jsonString);
		this.results = new LinkedHashMap<>();
		notifyAllObservers();
		logger.info(StringUtils.join(results.values(), ","));
		counters.setProblemList(results);
	}
}