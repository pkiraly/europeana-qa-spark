package com.nsdr.spark.model;

import java.util.Objects;
import org.apache.commons.lang3.StringUtils;

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

	public boolean hasValue() {
		return StringUtils.isNotBlank(value);
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public boolean hasLanguage() {
		return StringUtils.isNotBlank(language);
	}

	public String getResource() {
		return resource;
	}

	public void setResource(String resource) {
		this.resource = resource;
	}

	public boolean hasResource() {
		return StringUtils.isNotBlank(resource);
	}

	public boolean isEmpty() {
		return !hasValue() && !hasLanguage() && !hasResource();
	}

	@Override
	public String toString() {
		return "EdmFieldInstance{" + "value=" + value + ", language=" + language + ", resource=" + resource + '}';
	}

	@Override
	public int hashCode() {
		int hash = 5;
		hash = 19 * hash + Objects.hashCode(this.value);
		hash = 19 * hash + Objects.hashCode(this.language);
		hash = 19 * hash + Objects.hashCode(this.resource);
		return hash;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final EdmFieldInstance other = (EdmFieldInstance) obj;
		if (!Objects.equals(this.value, other.value)) {
			return false;
		}
		if (!Objects.equals(this.language, other.language)) {
			return false;
		}
		if (!Objects.equals(this.resource, other.resource)) {
			return false;
		}
		return true;
	}
	
	
}
