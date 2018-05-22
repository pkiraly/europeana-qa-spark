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
		assertEquals("oai:nid.pl:2", VocabularyUtils.extractPLD("oai:nid.pl:2:117/SP.1"));
		assertEquals("adlib.project", VocabularyUtils.extractPLD("adlib.project.20013695/SP.1"));
		assertEquals("280_place", VocabularyUtils.extractPLD("#POR_280_place01"));
		assertEquals("280_place", VocabularyUtils.extractPLD("#ES_280_place1"));
		assertEquals("geo:xy1", VocabularyUtils.extractPLD("geo:51.5844832546,4.77953553181"));
		assertEquals("geo:xy2", VocabularyUtils.extractPLD("geo:52379513.0,4900611.0"));
		assertEquals("UAEMAT", VocabularyUtils.extractPLD("UAEMAT-1/SP.1"));
		assertEquals("ort_herstellung_uuid", VocabularyUtils.extractPLD("ort_herstellung_uuid=41eb6b27-4607-0044-eea0-09a5f5bd28fc"));
		assertEquals("48.2d#latitude", VocabularyUtils.extractPLD("48.200525#latitude"));
		assertEquals("#LIT_placeN", VocabularyUtils.extractPLD("#LIT_place1"));
		assertEquals("#placeN", VocabularyUtils.extractPLD("#place1"));
		assertEquals("#place-abc", VocabularyUtils.extractPLD("#place-paris-barcelona"));
		assertEquals("SPANISH", VocabularyUtils.extractPLD("Abaceria Central, mercat de l'"));
		assertEquals("#1N.xy", VocabularyUtils.extractPLD("#16.38277715.077500"));
		assertEquals("#1N.xy", VocabularyUtils.extractPLD("#17.94478059.338983"));
		assertEquals("DASI:textT", VocabularyUtils.extractPLD("DASI:textT-5"));
		assertEquals("280", VocabularyUtils.extractPLD("NO_280_001#place"));
		assertEquals("#280", VocabularyUtils.extractPLD("#POL_280_008_place2"));
		assertEquals("#280", VocabularyUtils.extractPLD("#DEN_280_place"));
		assertEquals("N7", VocabularyUtils.extractPLD("0270330/SP.1"));
		assertEquals("N2", VocabularyUtils.extractPLD("62/SP.1"));
		assertEquals("N7-N", VocabularyUtils.extractPLD("4058167-6"));
		assertEquals("KE0", VocabularyUtils.extractPLD("KE042-057003-/SP.1"));
		assertEquals("KE0", VocabularyUtils.extractPLD("KE021-138----/SP.1"));
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
