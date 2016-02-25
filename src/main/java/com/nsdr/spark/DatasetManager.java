package com.nsdr.spark;

import java.util.Map;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class DatasetManager extends AbstractManager {

	public DatasetManager() {
		super();
		initialize("datasets.txt");
	}

	public Map<String, Integer> getDatasets() {
		return data;
	}
}
