package de.gwdg.europeanaqa.spark.bean;

public class Graph4PLD {
	String providerId;
	String type;
	String vocabulary;

	public Graph4PLD(String providerId, String type, String vocabulary) {
		this.providerId = providerId;
		this.type = type;
		this.vocabulary = vocabulary;
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

	public String getVocabulary() {
		return vocabulary;
	}

	public void setVocabulary(String vocabulary) {
		this.vocabulary = vocabulary;
	}
}
