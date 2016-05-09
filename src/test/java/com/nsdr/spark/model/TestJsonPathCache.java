package com.nsdr.spark.model;

import com.jayway.jsonpath.Configuration;
import com.nsdr.spark.TestUtils;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
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
public class TestJsonPathCache {

	Object jsonDoc;

	public TestJsonPathCache() throws IOException, URISyntaxException {
		String fileName = "problem-catalog/long-subject.json";
		Path path = Paths.get(getClass().getClassLoader().getResource(fileName).toURI());
		List<String> lines = Files.readAllLines(path, Charset.defaultCharset());
		String jsonString = lines.get(0);
		jsonDoc = Configuration.defaultConfiguration().jsonProvider().parse(jsonString);
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
	public void testSimpleValue() throws IOException, URISyntaxException {
		String jsonPath = "$.['ore:Proxy'][?(@['edm:europeanaProxy'][0] == 'false')]['dc:title']";

		JsonPathCache cache = new JsonPathCache();
		List<EdmFieldInstance> instances = cache.get(jsonDoc, jsonPath);

		assertNotNull(instances);
		assertEquals(1, instances.size());
		assertEquals("fiancata di biroccio", instances.get(0).getValue());
		assertNull(instances.get(0).getLanguage());
		assertNull(instances.get(0).getResource());
	}

	@Test
	public void testNonexistingField() throws IOException, URISyntaxException {
		String jsonPath = "$.['ore:Proxy'][?(@['edm:europeanaProxy'][0] == 'false')]['dc:title2']";

		JsonPathCache cache = new JsonPathCache();
		List<EdmFieldInstance> instances = cache.get(jsonDoc, jsonPath);

		assertNull(instances);
	}

	@Test
	public void testResourceField() throws IOException, URISyntaxException {
		String jsonPath = "$.['ore:Proxy'][?(@['edm:europeanaProxy'][0] == 'false')]['dcterms:isReferencedBy']";

		JsonPathCache cache = new JsonPathCache();
		List<EdmFieldInstance> instances = cache.get(jsonDoc, jsonPath);

		assertNotNull(instances);
		assertEquals(2, instances.size());
		assertEquals("http://sirpac.cultura.marche.it/web/Ricerca.aspx?ids=33334", 
				instances.get(0).getResource());
		assertNull(instances.get(0).getLanguage());
		assertNull(instances.get(0).getValue());
	}

	@Test
	public void testAbout() throws IOException, URISyntaxException {
		String jsonPath = "$.['edm:ProvidedCHO'][0]['@about']";

		JsonPathCache cache = new JsonPathCache();
		List<EdmFieldInstance> instances = cache.get(jsonDoc, jsonPath);

		assertNotNull(instances);
		assertEquals(1, instances.size());
		assertEquals("http://data.europeana.eu/item/07602/5CFC6E149961A1630BAD5C65CE3A683DEB6285A0", 
				instances.get(0).getValue());
		assertNull(instances.get(0).getLanguage());
		assertNull(instances.get(0).getResource());
	}
	
	@Test
	public void testLanguage() throws URISyntaxException, IOException {
		jsonDoc = TestUtils.buildDoc("problem-catalog/same-title-and-description.json");
		String jsonPath = "$.['edm:Place'][0]['skos:prefLabel']";

		
		JsonPathCache cache = new JsonPathCache();
		List<EdmFieldInstance> instances = cache.get(jsonDoc, jsonPath);

		assertNotNull(instances);
		assertEquals(117, instances.size());
		assertEquals("Holani", instances.get(0).getValue());
		assertEquals("to", instances.get(0).getLanguage());
		assertNull(instances.get(0).getResource());
		
	}

}
