package com.nsdr.spark.problemcatalog;

import com.jayway.jsonpath.Configuration;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class TestLongSubject {

	public TestLongSubject() {
	}

	@BeforeClass
	public static void setUpClass() {
	}

	@AfterClass
	public static void tearDownClass() {
	}

	@Before
	public void setUp() {
	}

	@After
	public void tearDown() {
	}

	@Test
	public void hello() throws IOException, URISyntaxException {
		String fileName = "problem-catalog/long-subject.json";
		Path path = Paths.get(getClass().getClassLoader().getResource(fileName).toURI());
		List<String> lines = Files.readAllLines(path, Charset.defaultCharset());
		String jsonString = lines.get(0);
		Object document = Configuration.defaultConfiguration()
				  .jsonProvider().parse(jsonString);
		ProblemCatalog problemCatalog = new ProblemCatalog();
		ProblemDetector detector = new LongSubject(problemCatalog);
		Map<String, Double> results = new HashMap<>();
		detector.update(document, results);
		assertEquals((Double)2.0, (Double)results.get("LongSubject"));
	}
}
