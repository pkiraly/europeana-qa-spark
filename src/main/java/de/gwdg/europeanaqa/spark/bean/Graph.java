package de.gwdg.europeanaqa.spark.bean;

public class Graph {
	String recordId;
	String type;
	String entityId;

	public Graph(String recordId, String type, String entityId) {
		this.recordId = recordId;
		this.type = type;
		this.entityId = entityId;
	}

	public String getRecordId() {
		return recordId;
	}

	public void setRecordId(String recordId) {
		this.recordId = recordId;
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
