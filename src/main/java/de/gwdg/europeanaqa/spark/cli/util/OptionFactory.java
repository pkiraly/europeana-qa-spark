package de.gwdg.europeanaqa.spark.cli.util;

import org.apache.commons.cli.Option;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class OptionFactory {

	public static Option create(String smallOpt, String longOpt, boolean required, String desc) {
		return create(smallOpt, longOpt, required, desc, true);
	}

	public static Option create(String smallOpt, String longOpt, boolean required, String desc, boolean hasArg) {
		Option option = Option.builder(smallOpt)
				       .longOpt(longOpt)
				       .required(required)
				       .desc(desc)
				       .hasArg(hasArg)
				       .build();
		return option;
	}

}
