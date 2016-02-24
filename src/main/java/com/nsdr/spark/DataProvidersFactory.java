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
public class DataProvidersFactory {

	private static Logger logger = Logger.getLogger(DataProvidersFactory.class.getCanonicalName());
	private static Map<String, Integer> dataProviders;
	
	public DataProvidersFactory() {
		initialize();
	}

	private void initialize() {
		try {
			Path path = Paths.get(getClass().getClassLoader().getResource("data-providers.txt").toURI());
			List<String> lines = Files.readAllLines(path, Charset.defaultCharset());
			dataProviders = new HashMap<>();
			int i = 1;
			for (String dataProvider : lines) {
				dataProviders.put(dataProvider, i++);
			}
		} catch (URISyntaxException | IOException ex) {
			logger.severe(ex.getLocalizedMessage());
		}
	}

	public Map<String, Integer> getDataProviders() {
		return dataProviders;
	}

	public Integer lookup(String dataProvider) {
		if (!dataProviders.containsKey(dataProvider)) {
			dataProviders.put(dataProvider, dataProviders.size() + 1);
		}
		return dataProviders.get(dataProvider);
	}

	private void save(String fileName) {
		
	}
}
