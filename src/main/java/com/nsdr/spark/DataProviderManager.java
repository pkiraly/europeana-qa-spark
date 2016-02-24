package com.nsdr.spark;

import java.util.Map;
import java.util.logging.Logger;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class DataProviderManager extends AbstractManager {

	private static Logger logger = Logger.getLogger(DataProviderManager.class.getCanonicalName());

	public DataProviderManager() {
		super();
		initialize("data-providers.txt");
		logger.info("data size: " + data.size());
	}

	public Map<String, Integer> getDataProviders() {
		return data;
	}

}
