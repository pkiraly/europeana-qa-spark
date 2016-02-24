
import com.jayway.jsonpath.InvalidJsonException;
import com.nsdr.spark.DataProvidersFactory;
import com.nsdr.spark.DatasetsFactory;
import com.nsdr.spark.JsonPathBasedCompletenessCounter;
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
import org.junit.Rule;
import org.junit.rules.ExpectedException;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class TestCounter {

	private JsonPathBasedCompletenessCounter counter;

	public TestCounter() {
	}

	@BeforeClass
	public static void setUpClass() {
	}

	@AfterClass
	public static void tearDownClass() {
	}

	@Before
	public void setUp() throws URISyntaxException, IOException {
		counter = new JsonPathBasedCompletenessCounter();
		counter.setDataProvidersFactory(new DataProvidersFactory());
		counter.setDatasetsFactory(new DatasetsFactory());
		counter.count(readFirstLine("test.json"));
	}

	public String readFirstLine(String fileName) throws URISyntaxException, IOException {
		Path path = Paths.get(getClass().getResource(fileName).toURI());
		List<String> lines = Files.readAllLines(path, Charset.defaultCharset());
		return lines.get(0);
	}

	@After
	public void tearDown() {
	}

	@Test
	public void testId() throws URISyntaxException, IOException {
		assertEquals("92062/BibliographicResource_1000126015451", counter.getRecordID());
	}

	@Test
	public void testDataProvider() throws URISyntaxException, IOException {
		assertEquals("Österreichische Nationalbibliothek - Austrian National Library", counter.getDataProvider());
		assertEquals("2", counter.getDataProviderCode());
	}

	@Test
	public void testDataset() throws URISyntaxException, IOException {
		assertEquals("92062_Ag_EU_TEL_a0480_Austria", counter.getDataset());
		assertEquals("1", counter.getDatasetCode());
	}

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testInvalidRecord() throws URISyntaxException, IOException {

		thrown.expect(InvalidJsonException.class);
		thrown.expectMessage("Unexpected character (:) at position 28");

		counter.count(readFirstLine("invalid.json"));
		fail("Should throw an exception if one or more of given numbers are negative");
	}

	@Test
	public void testFullResults() {
		assertEquals("2,92062/BibliographicResource_1000126015451,\"TOTAL\":0.416667,\"MANDATORY\":0.769231,\"DESCRIPTIVENESS\":0.181818,\"SEARCHABILITY\":0.388889,\"CONTEXTUALIZATION\":0.272727,\"IDENTIFICATION\":0.500000,\"BROWSING\":0.357143,\"VIEWING\":0.750000,\"REUSABILITY\":0.416667,\"MULTILINGUALITY\":0.400000", counter.getFullResults(true));
		assertEquals("2,92062/BibliographicResource_1000126015451,0.416667,0.769231,0.181818,0.388889,0.272727,0.500000,0.357143,0.750000,0.416667,0.400000", counter.getFullResults(false));
		assertEquals("2,92062/BibliographicResource_1000126015451,0.416667,0.769231,0.181818,0.388889,0.272727,0.5,0.357143,0.75,0.416667,0.4", counter.getFullResults(false, true));
	}

	@Test
	public void testRegex() {
		String value = "0.50000";
		value = value.replaceAll("([0-9])0+$", "$1").replaceAll("\\.0+$", ".0");
		assertEquals("0.5", value);
	}

}
