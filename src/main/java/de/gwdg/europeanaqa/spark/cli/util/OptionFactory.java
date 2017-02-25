package de.gwdg.europeanaqa.spark.cli.util;

import java.io.Serializable;
import org.apache.commons.cli.Option;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class OptionFactory implements Serializable {

	public static Option create(String smallOpt, String longOpt, boolean required,
			  String desc) {
		return OptionFactory.create(smallOpt, longOpt, required, desc, true);
	}

	public static Option create(String smallOpt, String longOpt, boolean required,
			  String desc, boolean hasArg) {
		Option option = Option.builder(smallOpt)
				       .longOpt(longOpt)
				       .required(required)
				       .desc(desc)
				       .hasArg(hasArg)
				       .build();
		return option;
	}

}
