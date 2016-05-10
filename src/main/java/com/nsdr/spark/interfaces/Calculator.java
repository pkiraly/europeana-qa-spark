package com.nsdr.spark.interfaces;

import com.nsdr.spark.counters.Counters;
import com.nsdr.spark.model.JsonPathCache;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public interface Calculator {

	public void calculate(JsonPathCache cache, Counters counters);
}
