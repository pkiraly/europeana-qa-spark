package com.nsdr.spark;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class AbstractManager implements Serializable {

	private static Logger logger = Logger.getLogger(AbstractManager.class.getCanonicalName());
	protected static Map<String, Integer> data;

	protected void initialize(String fileName) {
		try {
			Path path = Paths.get(getClass().getClassLoader().getResource(fileName).toURI());
			List<String> lines = Files.readAllLines(path, Charset.defaultCharset());
			data = new LinkedHashMap<>();
			int i = 1;
			for (String line : lines) {
				data.put(line, i++);
			}
		} catch (URISyntaxException | IOException ex) {
			logger.severe(ex.getLocalizedMessage());
		}
	}

	public Integer lookup(String dataset) {
		if (!data.containsKey(dataset)) {
			data.put(dataset, data.size() + 1);
		}
		return data.get(dataset);
	}

	public void save(String fileName) throws FileNotFoundException, UnsupportedEncodingException {
		PrintWriter writer = new PrintWriter(fileName, "UTF-8");
		for (Map.Entry<String, Integer> entry : data.entrySet()) {
			writer.println(String.format("%s;%d", entry.getKey(), entry.getValue()));
		}
		writer.close();
	}
}
