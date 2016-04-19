package com.nsdr.spark.problemcatalog;

import com.jayway.jsonpath.JsonPath;
import com.nsdr.spark.util.JsonUtils;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * See for example:
 * http://www.europeana.eu/portal/record/07602/5CFC6E149961A1630BAD5C65CE3A683DEB6285A0.json
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class LongSubject extends ProblemDetector implements Serializable {

	private static final Logger logger = Logger.getLogger(LongSubject.class.getCanonicalName());

	private final String NAME = "LongSubject";
	private final String PATH = "$.['ore:Proxy'][?(@['edm:europeanaProxy'][0] == 'false')]['dc:subject']";
	private final int MAX_LENGTH = 50;

	public LongSubject(ProblemCatalog problemCatalog) {
		this.problemCatalog = problemCatalog;
		this.problemCatalog.attach(this);
		logger.info("problemCatalog is null? " + (problemCatalog == null));
	}

	@Override
	public void update(Object jsonDocument, Map<String, Double> results) {
		double value = 0;
		Object subjectObj = JsonPath.read(jsonDocument, PATH);
		if (subjectObj != null) {
			List<String> subjects = JsonUtils.extractList(subjectObj);
			if (subjects.size() > 0) {
				for (String subject : subjects) {
					if (subject.length() > MAX_LENGTH) {
						value += 1;
					}
				}
			}
		}
		results.put(NAME, value);
	}
}