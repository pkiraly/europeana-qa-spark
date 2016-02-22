
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

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class TestCounter {

	public TestCounter() {
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
	public void testId() throws URISyntaxException, IOException {
		Path path = Paths.get(getClass().getResource("test.json").toURI());
		List<String> lines = Files.readAllLines(path, Charset.defaultCharset());
		JsonPathBasedCompletenessCounter counter = new JsonPathBasedCompletenessCounter();
		counter.count(lines.get(0));
		assertEquals("92062/BibliographicResource_1000126015451", counter.getRecordID());
	}

	@Test
	public void testDataProvider() throws URISyntaxException, IOException {
		Path path = Paths.get(getClass().getResource("test.json").toURI());
		List<String> lines = Files.readAllLines(path, Charset.defaultCharset());
		JsonPathBasedCompletenessCounter counter = new JsonPathBasedCompletenessCounter();
		counter.count(lines.get(0));
		assertEquals("Österreichische Nationalbibliothek - Austrian National Library", counter.getDataProvider());
		assertEquals("2", counter.getDataProviderCode());
	}
	
	
}
