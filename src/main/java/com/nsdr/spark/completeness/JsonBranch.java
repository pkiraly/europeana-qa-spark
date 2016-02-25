package com.nsdr.spark.completeness;

import com.jayway.jsonpath.Filter;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class JsonBranch {

	public enum Category {

		MANDATORY("mandatory"), DESCRIPTIVENESS("descriptiveness"),
		SEARCHABILITY("searchability"), CONTEXTUALIZATION("contextualization"),
		IDENTIFICATION("identification"), BROWSING("browsing"), VIEWING("viewing"),
		REUSABILITY("re-usability"), MULTILINGUALITY("multilinguality");

		private final String name;

		private Category(String name) {
			this.name = name;
		}
	};

	private String label;
	private String jsonPath;
	private List<Category> categories;
	private String solrFieldName;
	private Filter filter = null;

	public JsonBranch(String label, String jsonPath, String solrFieldName) {
		this.label = label;
		this.jsonPath = jsonPath;
		this.solrFieldName = solrFieldName;
	}

	public JsonBranch(String label, String jsonPath, Category... categories) {
		this.label = label;
		this.jsonPath = jsonPath;
		this.categories = Arrays.asList(categories);
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public String getJsonPath() {
		return jsonPath;
	}

	public void setJsonPath(String jsonPath) {
		this.jsonPath = jsonPath;
	}

	public List<Category> getCategories() {
		return categories;
	}

	public void setCategories(List<Category> categories) {
		this.categories = categories;
	}

	public String getSolrFieldName() {
		return solrFieldName;
	}

	public void setSolrFieldName(String solrFieldName) {
		this.solrFieldName = solrFieldName;
	}

	public Filter getFilter() {
		return filter;
	}

	public void setFilter(Filter filter) {
		this.filter = filter;
	}

	public boolean hasFilter() {
		return filter != null;
	}
}
