package com.nsdr.spark.problemcatalog;

import com.nsdr.spark.model.JsonPathCache;
import java.util.Map;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public abstract class ProblemDetector {

	protected ProblemCatalog problemCatalog;

	public abstract void update(JsonPathCache cache, Map<String, Double> results);
}
