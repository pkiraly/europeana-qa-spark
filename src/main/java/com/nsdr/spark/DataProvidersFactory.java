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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class DataProvidersFactory {

	private static Map<String, Integer> dataProviders;
	
	public DataProvidersFactory() {
		initialize();
	}

	private void initialize() {
		try {
			System.err.println("1: " + getClass().getClassLoader().getResource("data-providers.txt").toURI());
			Path path = Paths.get(getClass().getClassLoader().getResource("data-providers.txt").toURI());
			List<String> lines = Files.readAllLines(path, Charset.defaultCharset());
			dataProviders = new HashMap<>();
			int i = 1;
			for (String dataProvider : lines) {
				dataProviders.put(dataProvider, i++);
			}
		} catch (URISyntaxException ex) {
			Logger.getLogger(DataProvidersFactory.class.getName()).log(Level.SEVERE, null, ex);
		} catch (IOException ex) {
			Logger.getLogger(DataProvidersFactory.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	public Map<String, Integer> getDataProviders() {
		return dataProviders;
	}
}
