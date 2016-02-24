package com.nsdr.spark;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class DatasetsFactory {

	private static Logger logger = Logger.getLogger(DatasetsFactory.class.getCanonicalName());
	private static Map<String, Integer> datasets;
	
	public DatasetsFactory() {
		initialize();
	}

	private void initialize() {
		try {
			Path path = Paths.get(getClass().getClassLoader().getResource("datasets.txt").toURI());
			List<String> lines = Files.readAllLines(path, Charset.defaultCharset());
			datasets = new HashMap<>();
			int i = 1;
			for (String dataProvider : lines) {
				datasets.put(dataProvider, i++);
			}
		} catch (URISyntaxException | IOException ex) {
			logger.severe(ex.getLocalizedMessage());
		}
	}

	public Map<String, Integer> getDatasets() {
		return datasets;
	}

	public Integer lookup(String dataset) {
		if (!datasets.containsKey(dataset)) {
			datasets.put(dataset, datasets.size() + 1);
		}
		return datasets.get(dataset);
	}
}
