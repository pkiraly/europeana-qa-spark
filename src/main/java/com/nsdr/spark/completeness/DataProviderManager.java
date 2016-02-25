package com.nsdr.spark.completeness;

import java.util.Map;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class DataProviderManager extends AbstractManager {

	public DataProviderManager() {
		super();
		initialize("data-providers.txt");
	}

	public Map<String, Integer> getDataProviders() {
		return data;
	}
}
