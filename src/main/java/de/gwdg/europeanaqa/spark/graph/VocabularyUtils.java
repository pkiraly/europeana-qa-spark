package de.gwdg.europeanaqa.spark.graph;

public class VocabularyUtils {

	public static String extractPLD(String identifier) {
		String pld = identifier
			.replaceAll("https?://", "[pld]")
			.replaceAll("data.europeana.eu/agent/.*", "data.europeana.eu/agent/")
			.replaceAll("data.europeana.eu/place/.*", "data.europeana.eu/place/")
			.replaceAll("data.europeana.eu/concept/.*", "data.europeana.eu/concept/")
			.replaceAll("rdf.kulturarvvasternorrland.se/.*", "rdf.kulturarvvasternorrland.se")
			.replaceAll("d-nb.info/gnd/.*", "d-nb.info/gnd/")
			.replaceAll("^#person-.*", "#person")
			.replaceAll("^#agent_.*", "#agent")
			.replaceAll("^RM0001.PEOPLE.*", "RM0001.PEOPLE")
			.replaceAll("^RM0001.THESAU.*", "RM0001.THESAU")
			.replaceAll("^person_uuid.*", "person_uuid")
			.replaceAll("^MTB-PE-.*", "MTB-PE")
			.replaceAll("^#[0-9a-f]{8}-[0-9a-f]{4}-.*", "#uuid")
			.replaceAll("^[0-9a-f]{8}-[0-9a-f]{4}-.*", "uuid")
			.replaceAll("^#ort-dargestellt-[0-9a-f]{8}-[0-9a-f]{4}-.*", "#ort-dargestellt")
			.replaceAll("^#ort-herstellung-[0-9a-f]{8}-[0-9a-f]{4}-.*", "#ort-herstellung")
			.replaceAll("^#ort-fund-[0-9a-f]{8}-[0-9a-f]{4}-.*", "#ort-fund")
			.replaceAll("^HA\\d+/SP", "HA___/SP")
			.replaceAll("^iid:\\d{7}", "iid")
			.replaceAll("^iid:\\d{4}", "iid")
			.replaceAll("^iid:\\d{3}", "iid")
			.replaceAll("^#5[5-9]\\..*", "#5x.coord")
			.replaceAll("^#6[0-5]\\..*", "#6x.coord")
			.replaceAll("^#locationOf:nn.*", "#locationOf:nn")
			.replaceAll("^#placeOf:.*", "#placeOf")
			.replaceAll("^Rijksmonumentnummer.*", "Rijksmonumentnummer")
			.replaceAll("^urn:nbn:nl:ui:13-.*", "urn:nbn:nl:ui:13")
			.replaceAll("^KIVOTOS_CETI_.*", "KIVOTOS_CETI")
			.replaceAll("^DMS01-.*", "DMS01")
			.replaceAll("^DMS02-.*", "DMS02")
			.replaceAll("^UJAEN_HASSET_.*", "UJAEN_HASSET")
			.replaceAll("^5500\\d{5}/", "5500")
			// agent
			.replaceAll("^\\/direct\\/\\d*$/", "direct")
			// concept
			.replaceAll("^urn:Mood:.*", "urn:Mood")
			.replaceAll("^urn:Instrument:.*", "urn:Instrument")
			.replaceAll("^DASI:supL.*", "DASI:supL")
			.replaceAll("^context_\\d{4}.*", "context_yyyy")
			.replaceAll("^context_AUR_\\d{4}.*", "context_AUR_yyyy")
			.replaceAll("^context_SLA_OU_\\d{4}.*", "context_SLA_OU_yyyy")
			.replaceAll("^context_AT-KLA_.*", "context_AT-KLA")
			.replaceAll("^context_WienStClaraOSCI_.*", "context_WienStClaraOSCI")
			.replaceAll("^context_A_.*", "context_A")
			.replaceAll("^context_B_.*", "context_B")
			.replaceAll("^context_C_.*", "context_C")
			.replaceAll("^context_D_.*", "context_D")
			.replaceAll("^context_E_.*", "context_E")
			.replaceAll("^context_F_.*", "context_F")
			.replaceAll("^context_G_.*", "context_G")
			.replaceAll("^context_H_.*", "context_H")
			.replaceAll("^context_I_.*", "context_I")
			.replaceAll("^context_K_.*", "context_K")
			.replaceAll("^context_L_.*", "context_L")
			.replaceAll("^context_M_.*", "context_M")
			.replaceAll("^context_O_.*", "context_O")
			.replaceAll("^context_P_.*", "context_P")
			.replaceAll("^context_.*", "context")
			.replaceAll("^#concept-.*", "#concept")

			// timespan
			.replaceAll("^#Timesspan_OpenUp!.*", "#Timesspan_OpenUp!")
			.replaceAll("^#57620.*", "#57620")
			.replaceAll("^#datierung-[0-9a-f]{8}-[0-9a-f]{4}-.*", "#datierung")
			.replaceAll("^datierung_uuid=[0-9a-f]{8}-[0-9a-f]{4}-.*", "datierung_uuid")
			;

		if (!pld.contains("data.europeana.eu/agent/")
			&& !pld.contains("data.europeana.eu/place/")
			&& !pld.contains("data.europeana.eu/concept/")
			&& !pld.contains("d-nb.info/gnd/")) {
			pld = pld.replaceAll("/.+", "/");
		}

		if (pld == null)
			pld = "";

		return pld;
	}

	public static String sanitize(String entityID) {
		return entityID.replaceAll("'", "\\\\'");
	}

}
