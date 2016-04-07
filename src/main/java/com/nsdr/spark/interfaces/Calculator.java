package com.nsdr.spark.interfaces;

import com.nsdr.spark.completeness.Counters;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public interface Calculator {

	public void calculate(String jsonString, Counters counters);
}
