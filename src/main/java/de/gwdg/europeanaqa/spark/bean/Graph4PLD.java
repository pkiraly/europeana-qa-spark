package de.gwdg.europeanaqa.spark.bean;

public class Graph4PLD {
	String providerId;
	String type;
	String entityId;

	public Graph4PLD(String providerId, String type, String entityId) {
		this.providerId = providerId;
		this.type = type;
		this.entityId = entityId;
	}

	public String getProviderId() {
		return providerId;
	}

	public void setProviderId(String providerId) {
		this.providerId = providerId;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getEntityId() {
		return entityId;
	}

	public void setEntityId(String entityId) {
		this.entityId = entityId;
	}
}
