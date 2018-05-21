package de.gwdg.europeanaqa.spark.graph;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class VocabularyUtilsTest {

	@Test
	public void testExtractPLD() {
		String original = "direct/9932";
		assertEquals(
			"direct",
			VocabularyUtils.extractPLD(original)
		);
	}

}
