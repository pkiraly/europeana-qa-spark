package de.gwdg.europeanaqa.spark.cli.util;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EuropeanaRecordReaderAPIClientTest {

	@Test
	public void testGetRecord() throws Exception {
		EuropeanaRecordReaderAPIClient rest = new EuropeanaRecordReaderAPIClient("144.76.218.178:8080");
		String recordId = "/91001/006ED5FBDCEF5B1847C443CA4829A65ABE55A917";
		String recordInJson = rest.getRecord(recordId);
		JSONObject record = (JSONObject) JSONValue.parse(recordInJson);
		assertEquals(recordId, record.get("identifier"));
	}

	@Test
	public void testGetRecord2() throws Exception {
		EuropeanaRecordReaderAPIClient rest = new EuropeanaRecordReaderAPIClient("144.76.218.178:8080");
		String recordId = "/91001/006ED5FBDCEF5B1847C443CA4829A65ABE55A917";
		String recordInJson = rest.getRecord2(recordId);
		JSONObject record = (JSONObject) JSONValue.parse(recordInJson);
		assertEquals(recordId, record.get("identifier"));
	}
}
