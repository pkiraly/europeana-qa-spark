package com.nsdr.spark.problemcatalog;

import com.jayway.jsonpath.JsonPath;
import com.nsdr.spark.util.JsonUtils;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class EmptyStrings extends ProblemDetector implements Serializable {

	private static final Logger logger = Logger.getLogger(EmptyStrings.class.getCanonicalName());

	private final String NAME = "EmptyStrings";
	private final List<String> paths = Arrays.asList(
		"$.['ore:Proxy'][?(@['edm:europeanaProxy'][0] == 'false')]['dc:title']",
		"$.['ore:Proxy'][?(@['edm:europeanaProxy'][0] == 'false')]['dc:description']",
		"$.['ore:Proxy'][?(@['edm:europeanaProxy'][0] == 'false')]['dc:subject']"
	);

	public EmptyStrings(ProblemCatalog problemCatalog) {
		this.problemCatalog = problemCatalog;
		this.problemCatalog.attach(this);
		logger.info("problemCatalog is null? " + (problemCatalog == null));
	}

	@Override
	public void update(Object jsonDocument, Map<String, Double> results) {
		logger.info("problemCatalog is null? " + (problemCatalog == null));
		double value = 0;
		for (String path : paths) {
			Object subjectObj = JsonPath.read(jsonDocument, path);
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
