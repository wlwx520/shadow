package com.track.shadow;

import org.apache.log4j.Logger;

public class LogServer {
	private static Logger logger = Logger.getLogger(Logger.class);

	public static void log(String log) {
		logger.info(log);
	}
}
