package com.nsdr.spark.model;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class EdmFieldInstance {

	private String value;
	private String language;
	private String resource;

	public EdmFieldInstance() {
	}

	public EdmFieldInstance(String value) {
		this.value = value;
	}

	public EdmFieldInstance(String value, String language) {
		this.value = value;
		this.language = language;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public String getResource() {
		return resource;
	}

	public void setResource(String resource) {
		this.resource = resource;
	}

	public boolean isEmpty() {
		return (value == null    || value.isEmpty())
		    && (language == null || language.isEmpty())
		    && (resource == null || resource.isEmpty());
	}

	@Override
	public String toString() {
		return "EdmFieldInstance{" + "value=" + value + ", language=" + language + ", resource=" + resource + '}';
	}
}
