
import com.jayway.jsonpath.InvalidJsonException;
import com.nsdr.spark.DataProvidersFactory;
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
		counter.setDataProviders(new DataProvidersFactory().getDataProviders());
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

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testInvalidRecord() throws URISyntaxException, IOException {

		thrown.expect(InvalidJsonException.class);
		thrown.expectMessage("Unexpected character (:) at position 28");

		counter.count(readFirstLine("invalid.json"));
		fail("Should throw an exception if one or more of given numbers are negative");
	}

}
