package com.nsdr.spark.problemcatalog;

import com.jayway.jsonpath.JsonPath;
import com.nsdr.spark.util.JsonUtils;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class EmptyStrings extends ProblemDetector {

	private final String NAME = "EmptyStrings";
	private final List<String> paths = Arrays.asList(
		"$.['ore:Proxy'][?(@['edm:europeanaProxy'][0] == 'false')]['dc:title']",
		"$.['ore:Proxy'][?(@['edm:europeanaProxy'][0] == 'false')]['dc:description']",
		"$.['ore:Proxy'][?(@['edm:europeanaProxy'][0] == 'false')]['dc:subject']"
	);

	public EmptyStrings(ProblemCatalog problemCatalog) {
		this.problemCatalog = problemCatalog;
		this.problemCatalog.attach(this);
	}

	@Override
	public void update(Map<String, Double> results) {
		double value = 0;
		for (String path : paths) {
			Object subjectObj = JsonPath.read(problemCatalog.getJsonDocument(), path);
			if (subjectObj != null) {
				List<String> subjects = JsonUtils.extractList(subjectObj);
				if (subjects.size() > 0) {
					for (String subject : subjects) {
						if (subject.length() == 0) {
							value += 1;
						}
					}
				}
			}
		}
		results.put(NAME, value);
	}
}
