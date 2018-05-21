package de.gwdg.europeanaqa.spark.graph;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class VocabularyUtilsTest {

	@Test
	public void testExtractPLD() {
		String original = "/direct/9932";
		assertEquals(
			"direct",
			VocabularyUtils.extractPLD(original)
		);
	}

	@Test
	public void testSanitize() {
		String original = "http://iconclass.org/41D221(FOOL'S CAP)(+83)";
		assertEquals(
			"http://iconclass.org/41D221(FOOL\\'S CAP)(+83)",
			VocabularyUtils.sanitize(original)
		);
	}
}
