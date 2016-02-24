package com.nsdr.spark;

import java.util.Map;
import java.util.logging.Logger;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class DatasetManager extends AbstractManager {

	private static Logger logger = Logger.getLogger(DatasetManager.class.getCanonicalName());
	private static Map<String, Integer> data;

	public DatasetManager() {
		initialize("datasets.txt");
	}

	public Map<String, Integer> getDatasets() {
		return data;
	}

}
