package com.nsdr.spark.problemcatalog;

import com.jayway.jsonpath.JsonPath;
import com.nsdr.spark.util.JsonUtils;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class TitleAndDescriptionAreSame extends ProblemDetector implements Serializable {

	private static final Logger logger = Logger.getLogger(TitleAndDescriptionAreSame.class.getCanonicalName());

	private final String NAME = "TitleAndDescriptionAreSame";
	private final String title = "$.['ore:Proxy'][?(@['edm:europeanaProxy'][0] == 'false')]['dc:title']";
	private final String description = "$.['ore:Proxy'][?(@['edm:europeanaProxy'][0] == 'false')]['dc:description']";

	public TitleAndDescriptionAreSame(ProblemCatalog problemCatalog) {
		this.problemCatalog = problemCatalog;
		this.problemCatalog.attach(this);
		logger.info("problemCatalog is null? " + (problemCatalog == null));
	}

	@Override
	public void update(Map<String, Double> results) {
		logger.info("problemCatalog is null? " + (problemCatalog == null));
		double value = 0;
		Object titlesObj = JsonPath.read(problemCatalog.getJsonDocument(), title);
		if (titlesObj != null) {
			Object descriptionObj = JsonPath.read(problemCatalog.getJsonDocument(), description);
			if (descriptionObj != null) {
				List<String> titles = JsonUtils.extractList(titlesObj);
				if (titles.size() > 0) {
					List<String> descriptions = JsonUtils.extractList(descriptionObj);
					if (descriptions.size() > 0) {
						for (String title : titles) {
							if (descriptions.contains(title)) {
								value = 1;
								break;
							}
						}
					}
				}
			}
		}
		results.put(NAME, value);
	}
}
