package de.gwdg.europeanaqa.spark.bean;

import java.util.List;

public class Vocabulary {
	String providerId;
	String type;
	String vocabulary;
	List<Integer> cardinality;

	public Vocabulary(String providerId, String type, String vocabulary, List<Integer> cardinality) {
		this.providerId = providerId;
		this.type = type;
		this.vocabulary = vocabulary;
		this.cardinality = cardinality;
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

	public List<Integer> getCardinality() {
		return cardinality;
	}

	public void setCardinality(List<Integer> cardinality) {
		this.cardinality = cardinality;
	}
}
