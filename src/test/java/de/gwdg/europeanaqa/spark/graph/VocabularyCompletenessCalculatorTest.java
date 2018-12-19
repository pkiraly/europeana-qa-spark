package de.gwdg.europeanaqa.spark.graph;

import de.gwdg.europeanaqa.api.model.Format;
import de.gwdg.europeanaqa.spark.TestUtils;
import de.gwdg.europeanaqa.spark.bean.Vocabulary;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class VocabularyCompletenessCalculatorTest {


	@Test
	public void test() throws IOException, URISyntaxException {
		VocabularyCompletenessCalculator calculator = new VocabularyCompletenessCalculator(Format.FULLBEAN);
		String jsonString = TestUtils.readFirstLine("edm-fullbean2.json");
		List<Vocabulary> vocabs = calculator.calculate(jsonString);

		assertEquals("http://semium.org/time/19xx_1_third", vocabs.get(0).getEntityId());
		assertEquals(2, vocabs.get(0).getLinkage());

		assertEquals("http://semium.org/time/AD2xxx", vocabs.get(1).getEntityId());
		assertEquals(0, vocabs.get(1).getLinkage());

		assertEquals("http://semium.org/time/1909", vocabs.get(2).getEntityId());
		assertEquals(2, vocabs.get(2).getLinkage());

		assertEquals("http://semium.org/time/ChronologicalPeriod", vocabs.get(3).getEntityId());
		assertEquals(0, vocabs.get(3).getLinkage());

		assertEquals("http://semium.org/time/19xx", vocabs.get(4).getEntityId());
		assertEquals(2, vocabs.get(4).getLinkage());

		assertEquals("http://semium.org/time/Time", vocabs.get(5).getEntityId());
		assertEquals(0, vocabs.get(5).getLinkage());
	}

}
