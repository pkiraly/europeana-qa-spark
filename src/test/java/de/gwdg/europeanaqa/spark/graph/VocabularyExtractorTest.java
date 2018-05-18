package de.gwdg.europeanaqa.spark.graph;

import de.gwdg.europeanaqa.spark.TestUtils;
import de.gwdg.metadataqa.api.model.JsonPathCache;
import de.gwdg.metadataqa.api.model.XmlFieldInstance;
import de.gwdg.metadataqa.api.schema.EdmFullBeanSchema;
import de.gwdg.metadataqa.api.schema.Schema;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class VocabularyExtractorTest {

	@Test
	public void test() throws IOException, URISyntaxException {
		String agentId = "http://data.europeana.eu/agent/base/151990";
		String jsonString = TestUtils.readFirstLine("edm-fullbean.json");
		Schema schema = new EdmFullBeanSchema();

		VocabularyExtractor e = new VocabularyExtractor(schema);
		JsonPathCache<? extends XmlFieldInstance> cache = new JsonPathCache<>(jsonString);
		List<Integer> cardinality = e.getCardinality(cache, "agent", agentId);
		List<Integer> expected = Arrays.asList(new Integer[]{
			1, 0, 1, 0, 0, 22, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 13, 13, 1, 0
		});
		assertEquals(expected, cardinality);
	}
}
