package de.gwdg.europeanaqa.spark.bean;

public class Vocabulary {
	String providerId;
	String entityType;
	String entityId;
	String vocabulary;
	String cardinality;
	int linkage;

	public Vocabulary(String providerId, String entityType, String entityId, String vocabulary, String cardinality) {
		this.providerId = providerId;
		this.entityType = entityType;
		this.entityId = entityId;
		this.vocabulary = vocabulary;
		this.cardinality = cardinality;
	}

	public String getProviderId() {
		return providerId;
	}

	public void setProviderId(String providerId) {
		this.providerId = providerId;
	}

	public String getEntityType() {
		return entityType;
	}

	public void setEntityType(String entityType) {
		this.entityType = entityType;
	}

	public String getEntityId() {
		return entityId;
	}

	public void setEntityId(String entityId) {
		this.entityId = entityId;
	}

	public String getVocabulary() {
		return vocabulary;
	}

	public void setVocabulary(String vocabulary) {
		this.vocabulary = vocabulary;
	}

	public String getCardinality() {
		return cardinality;
	}

	public void setCardinality(String cardinality) {
		this.cardinality = cardinality;
	}

	public int getLinkage() {
		return linkage;
	}

	public void setLinkage(int linkage) {
		this.linkage = linkage;
	}
}
