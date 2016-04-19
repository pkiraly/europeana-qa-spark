package com.nsdr.spark.problemcatalog;

import java.util.Map;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public abstract class ProblemDetector {

	protected ProblemCatalog problemCatalog;

	public abstract void update(Map<String, Double> results);
}
