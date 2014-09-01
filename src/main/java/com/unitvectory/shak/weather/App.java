package com.unitvectory.shak.weather;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

/**
 * The main application.
 * 
 * @author Jared Hatfield
 * 
 */
public class App {

	/**
	 * the log
	 */
	private static Logger log = Logger.getLogger(App.class);

	/**
	 * the app config
	 */
	private static AppConfig config;

	/**
	 * The main application.
	 * 
	 * @param args
	 *            the args
	 */
	public static void main(String[] args) {
		// create the parser
		org.apache.commons.cli.CommandLineParser parser = new BasicParser();
		try {
			// The required options
			Options options = new Options();
			@SuppressWarnings("static-access")
			Option configOption = OptionBuilder.withArgName("config").hasArg()
					.withDescription("path to config file").isRequired()
					.create("config");
			options.addOption(configOption);

			// Parse the command line arguments
			CommandLine line = parser.parse(options, args);
			String configPath = line.getOptionValue("config");

			// Load the config
			config = AppConfig.load(configPath);
			if (config == null) {
				return;
			}

		} catch (ParseException exp) {
			System.err.println("Parsing failed.  Reason: " + exp.getMessage());
		}
	}
}
