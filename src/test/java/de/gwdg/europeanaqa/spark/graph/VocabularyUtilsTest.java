package de.gwdg.europeanaqa.spark.graph;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class VocabularyUtilsTest {

	@Test
	public void testExtractPLD() {
		assertEquals("direct", VocabularyUtils.extractPLD("/direct/9932"));
		assertEquals("THESAURUS_CETI", VocabularyUtils.extractPLD("THESAURUS_CETI_9/SP.1"));
		assertEquals("6ddd", VocabularyUtils.extractPLD("6629/SP.1"));
		assertEquals("ddd", VocabularyUtils.extractPLD("162/SP.1"));
		assertEquals("dddd", VocabularyUtils.extractPLD("4881/SP.1"));
		assertEquals("2.ddd", VocabularyUtils.extractPLD("2,294/SP.1"));
		assertEquals("1.ddd", VocabularyUtils.extractPLD("1,167/SP.1"));
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
