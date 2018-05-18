package de.gwdg.europeanaqa.spark.graph;

import de.gwdg.metadataqa.api.json.JsonBranch;
import de.gwdg.metadataqa.api.model.JsonPathCache;
import de.gwdg.metadataqa.api.model.XmlFieldInstance;
import de.gwdg.metadataqa.api.schema.Schema;

import java.io.Serializable;
import java.util.*;

public class VocabularyExtractor implements Serializable {

	private Schema qaSchema;
	private final Map<String, JsonBranch> entitiesPaths = new HashMap<>();

	public VocabularyExtractor(Schema qaSchema) {
		this.qaSchema = qaSchema;
		entitiesPaths.put("agent", qaSchema.getPathByLabel("Agent"));
		entitiesPaths.put("concept", qaSchema.getPathByLabel("Concept"));
		entitiesPaths.put("place", qaSchema.getPathByLabel("Place"));
		entitiesPaths.put("timespan", qaSchema.getPathByLabel("Timespan"));
	}

	public List<Integer> getCardinality(JsonPathCache<? extends XmlFieldInstance> cache, String entityType, String entityID) {
		String entityById = entitiesPaths.get(entityType).getJsonPath() + "[?(@['about'] == '" + entityID + "')]";
		List<JsonBranch> childrenPaths = entitiesPaths.get(entityType).getChildren();

		List<Integer> cardinalities = new ArrayList<>();
		Object entity = cache.getFragment(entityById);
		for (JsonBranch childPath : childrenPaths) {
			List<? extends XmlFieldInstance> fieldInstances = cache.get(childPath.getJsonPath(), childPath.getJsonPath(), entity);
			cardinalities.add(fieldInstances != null ? fieldInstances.size() : 0);
		}

		return cardinalities;
	}
}
