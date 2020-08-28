package de.gwdg.europeanaqa.spark.graph;

import de.gwdg.europeanaqa.api.abbreviation.EdmDataProviderManager;
import de.gwdg.europeanaqa.api.calculator.MultiFieldExtractor;
import de.gwdg.europeanaqa.api.model.Format;
import de.gwdg.europeanaqa.spark.bean.Vocabulary;
import de.gwdg.metadataqa.api.json.JsonBranch;
import de.gwdg.metadataqa.api.model.EdmFieldInstance;
import de.gwdg.metadataqa.api.model.pathcache.JsonPathCache;
import de.gwdg.metadataqa.api.model.XmlFieldInstance;
import de.gwdg.metadataqa.api.schema.EdmFullBeanSchema;
import de.gwdg.metadataqa.api.schema.EdmOaiPmhXmlSchema;
import de.gwdg.metadataqa.api.schema.Schema;
import net.minidev.json.JSONArray;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.*;
import java.util.logging.Logger;

public class VocabularyCompletenessCalculator implements Serializable {

	private static final Logger logger = Logger.getLogger(VocabularyCompletenessCalculator.class.getCanonicalName());

	public enum EntityType {
		AGENT, CONCEPT, PLACE, TIMESPAN
	}

	private static final List<String> enrichableFieldLabels = Arrays.asList(
		"Proxy/dc:contributor", "Proxy/dc:publisher", "Proxy/dc:creator",
		"Proxy/dc:subject", "Proxy/dc:type", "Proxy/edm:hasType", "Proxy/dc:coverage",
		"Proxy/dcterms:spatial", "Proxy/edm:currentLocation", "Proxy/dc:date",
		"Proxy/dcterms:created", "Proxy/dcterms:issued", "Proxy/dcterms:temporal",
		"Proxy/edm:hasMet",
		"Proxy/dc:format", "Proxy/dcterms:conformsTo",
		"Proxy/dcterms:medium", "Proxy/edm:isRelatedTo"
	);
	List<String> enrichableFieldPaths = new ArrayList<>();

	Map<String, EntityType> contextualIds;
	Schema qaSchema = null;
	JsonBranch providerProxyBranch = null;
	JsonBranch europeanaProxyBranch = null;

	Object providerProxy = null;
	Object europeanaProxy = null;

	Map<String, String> extractableFields = new LinkedHashMap<>();
	final MultiFieldExtractor fieldExtractor;
	final VocabularyExtractor vocabularyExtractor;
	Map<String, String> cardinalityMap = new HashMap<>();
	Map<String, String> vocabNameMap = new HashMap<>();
	private static final EdmDataProviderManager dataProviderManager = new EdmDataProviderManager();
	private static final List<String> entities = Arrays.asList("agent", "concept", "place", "timespan");

	public VocabularyCompletenessCalculator(Format format) {
		setSchema(format);
		fieldExtractor = new MultiFieldExtractor(qaSchema);
		vocabularyExtractor = new VocabularyExtractor(qaSchema);
	}

	public List<Vocabulary> calculate(String jsonString) {
		providerProxy = null;
		europeanaProxy = null;

		List<Vocabulary> values = new ArrayList<>();
		JsonPathCache<? extends XmlFieldInstance> cache = new JsonPathCache<>(jsonString);
		fieldExtractor.measure(cache);
		Map<String, ? extends Object> map = fieldExtractor.getResultMap();
		String recordId = ((List<String>) map.get("recordId")).get(0);

		String dataProvider = extractValue(map, "dataProvider");
		String provider = extractValue(map, "provider");

		String providerId = (dataProvider != null)
			? getDataProviderCode(dataProvider)
			: (provider != null ? getDataProviderCode(provider) : "0");

		for (String entityType : entities) {
			for (String entityID : (List<String>) map.get(entityType)) {
				String cardinality = getOrCreateCardinality(cache, entityType, entityID);
				String vocabularyID = getOrCreateVocabulary(recordId, entityType, entityID);

				Vocabulary vocabulary = new Vocabulary(
					providerId,
					entityType,
					entityID,
					vocabularyID,
					cardinality
				);
				vocabulary.setLinkage(detectLinkage(cache, entityType, entityID));
				values.add(vocabulary);
			}
		}
		return values;
	}

	private int detectLinkage(JsonPathCache<? extends XmlFieldInstance> cache, String entityType, String entityID) {
		if (providerProxy == null) {
			providerProxy = cache.getFragment(providerProxyBranch.getJsonPath());
			if (providerProxy == null) {
				logger.severe(String.format("Entity is null"));
			} else {
				if (providerProxy instanceof JSONArray) {
					providerProxy = ((JSONArray) providerProxy).get(0);
				}
			}
		}
		if (europeanaProxy == null) {
			europeanaProxy = cache.getFragment(europeanaProxyBranch.getJsonPath());
			if (europeanaProxy == null) {
				logger.severe(String.format("Entity is null"));
			} else {
				if (europeanaProxy instanceof JSONArray) {
					europeanaProxy = ((JSONArray) europeanaProxy).get(0);
				}
			}
		}

		int linkage = 0;
		List<? extends XmlFieldInstance> fieldInstances;
		for (String path : enrichableFieldPaths) {
			fieldInstances = cache.get(path, path, providerProxy);
			if (fieldInstances != null && !fieldInstances.isEmpty()) {
				for (EdmFieldInstance instance : (List<EdmFieldInstance>) fieldInstances) {
					if (instance != null && isLinkToEntity(instance, entityID)) {
						linkage = 1;
						break;
					}
				}
			}
			fieldInstances = cache.get(path + "eu", path, europeanaProxy);
			if (fieldInstances != null && !fieldInstances.isEmpty()) {
				for (EdmFieldInstance instance : (List<EdmFieldInstance>) fieldInstances) {
					if (instance != null && isLinkToEntity(instance, entityID)) {
						linkage = (linkage == 0) ? 2 : 3;
						break;
					}
				}
			}
			if (linkage != 0)
				break;
		}
		return linkage;
	}

	private String getOrCreateVocabulary(String recordId, String entityType, String entityID) {
		String vocabulary = null;
		if (vocabNameMap.containsKey(entityID)) {
			vocabulary = vocabNameMap.get(entityID);
		} else {
			vocabulary = VocabularyUtils.extractPLD(entityID);
			if (vocabulary.equals(entityID) && !vocabulary.equals("#place")) {
				logger.severe(String.format("%s -- Undetected vocabulary (%s): %s", recordId, entityType, entityID));
			}
			vocabNameMap.put(entityID, vocabulary);
		}
		return vocabulary;
	}

	private String getOrCreateCardinality(JsonPathCache<? extends XmlFieldInstance> cache, String entityType, String entityID) {
		String cardinality = null;
		if (cardinalityMap.containsKey(entityID)) {
			cardinality = cardinalityMap.get(entityID);
		} else {
			List<Integer> cardinalities = vocabularyExtractor.getCardinality(cache, entityType, entityID);
			cardinality = StringUtils.join(cardinalities, ",");
			cardinalityMap.put(entityID, cardinality);
		}
		return cardinality;
	}

	public static String getDataProviderCode(String dataProvider) {
		String dataProviderCode;
		if (dataProvider == null) {
			dataProviderCode = "0";
		} else if (dataProviderManager != null) {
			dataProviderCode = String.valueOf(dataProviderManager.lookup(dataProvider));
		} else {
			dataProviderCode = dataProvider;
		}
		return dataProviderCode;
	}

	private static String extractValue(Map map, String key) {
		String value = null;
		if (map.get(key) != null && !((List<String>) map.get(key)).isEmpty())
			value = ((List<String>) map.get(key)).get(0);
		return value;
	}

	public void setSchema(Format format) {
		if (format == null || format.equals(Format.OAI_PMH_XML)) {
			qaSchema = new EdmOaiPmhXmlSchema();
			extractableFields.put("recordId", "$.identifier");
			extractableFields.put("dataProvider", "$.['ore:Aggregation'][0]['edm:dataProvider'][0]");
			extractableFields.put("provider", "$.['ore:Aggregation'][0]['edm:provider'][0]");
			extractableFields.put("agent", "$.['edm:Agent'][*]['@about']");
			extractableFields.put("concept", "$.['skos:Concept'][*]['@about']");
			extractableFields.put("place", "$.['edm:Place'][*]['@about']");
			extractableFields.put("timespan", "$.['edm:TimeSpan'][*]['@about']");
		} else {
			qaSchema = new EdmFullBeanSchema();
			extractableFields.put("recordId", "$.identifier");
			extractableFields.put("dataProvider", "$.['aggregations'][0]['edmDataProvider'][0]");
			extractableFields.put("provider", "$.['aggregations'][0]['edmProvider'][0]");
			extractableFields.put("agent", "$.['agents'][*]['about']");
			extractableFields.put("concept", "$.['concepts'][*]['about']");
			extractableFields.put("place", "$.['places'][*]['about']");
			extractableFields.put("timespan", "$.['timespans'][*]['about']");
		}
		qaSchema.setExtractableFields(extractableFields);

		providerProxyBranch = qaSchema.getPathByLabel("Proxy");
		europeanaProxyBranch = null;
		try {
			europeanaProxyBranch = (JsonBranch) providerProxyBranch.clone();
			europeanaProxyBranch.setJsonPath(providerProxyBranch.getJsonPath().replace("false", "true"));
		} catch (CloneNotSupportedException ex) {
			logger.severe(ex.getMessage());
		}

		for (String label : enrichableFieldLabels) {
			enrichableFieldPaths.add(qaSchema.getPathByLabel(label).getJsonPath());
		}
	}

	private boolean isLinkToEntity(EdmFieldInstance field, String entityID) {
		if (field.hasResource() && entityID.trim().equals(field.getResource().trim())) {
			return true;
		}
		return entityID.trim().equals(field.getValue().trim());
	}

}
