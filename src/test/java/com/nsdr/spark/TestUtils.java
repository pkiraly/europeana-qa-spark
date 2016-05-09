package com.nsdr.spark;

import com.jayway.jsonpath.Configuration;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class TestUtils {

	public static Object buildDoc(String fileName) throws URISyntaxException, IOException {
		Path path = Paths.get(TestUtils.class.getClassLoader().getResource(fileName).toURI());
		List<String> lines = Files.readAllLines(path, Charset.defaultCharset());
		String jsonString = lines.get(0);
		Object jsonDoc = Configuration.defaultConfiguration().jsonProvider().parse(jsonString);
		return jsonDoc;
	}
}
