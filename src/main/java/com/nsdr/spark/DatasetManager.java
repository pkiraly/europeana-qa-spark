package com.nsdr.spark;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class DatasetManager extends AbstractManager {

	private static Logger logger = Logger.getLogger(DatasetManager.class.getCanonicalName());

	public DatasetManager() {
		super();
		initialize("datasets.txt");
		logger.info("data size: " + data.size());
	}

	public Map<String, Integer> getDatasets() {
		return data;
	}

}
