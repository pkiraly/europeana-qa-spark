package de.gwdg.europeanaqa.spark.graph;

import de.gwdg.metadataqa.api.json.JsonBranch;
import de.gwdg.metadataqa.api.model.JsonPathCache;
import de.gwdg.metadataqa.api.model.XmlFieldInstance;
import de.gwdg.metadataqa.api.schema.Schema;
import net.minidev.json.JSONArray;

import java.io.Serializable;
import java.util.*;
import java.util.logging.Logger;

public class VocabularyExtractor implements Serializable {

	private static final Logger logger = Logger.getLogger(VocabularyExtractor.class.getCanonicalName());

	private Schema qaSchema;
	private final Map<String, JsonBranch> entitiesPaths = new HashMap<>();

	public VocabularyExtractor(Schema qaSchema) {
		this.qaSchema = qaSchema;
		entitiesPaths.put("agent", qaSchema.getPathByLabel("Agent"));
		entitiesPaths.put("concept", qaSchema.getPathByLabel("Concept"));
		entitiesPaths.put("place", qaSchema.getPathByLabel("Place"));
		entitiesPaths.put("timespan", qaSchema.getPathByLabel("Timespan"));
	}

	public List<Integer> getCardinality(
		JsonPathCache<? extends XmlFieldInstance> cache,
		String entityType,
		String entityID)
	{
		String entityById = entitiesPaths.get(entityType).getJsonPath()
			+ "[?(@['about'] == '" + entityID.replaceAll("'", "\\'") + "')]";

		List<Integer> cardinalities = new ArrayList<>();
		Object entities = cache.getFragment(entityById);
		if (entities == null) {
			logger.severe(String.format("Entity is null"));
		} else {
			if (entities instanceof JSONArray) {
				Object entity = ((JSONArray) entities).get(0);
				List<JsonBranch> childrenPaths = entitiesPaths.get(entityType).getChildren();
				for (JsonBranch childPath : childrenPaths) {
					List<? extends XmlFieldInstance> fieldInstances = cache.get(
						childPath.getJsonPath(), childPath.getJsonPath(), entity);
					cardinalities.add(fieldInstances != null ? fieldInstances.size() : 0);
				}
			} else {
				logger.severe(String.format("Entity is not a JSONArray"));
			}
		}

		return cardinalities;
	}
}
