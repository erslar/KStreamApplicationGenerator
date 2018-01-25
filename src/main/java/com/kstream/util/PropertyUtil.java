package com.kstream.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertyUtil {

	/** The value containing LOGGER. */
	static final Logger Properties = LoggerFactory.getLogger(PropertyUtil.class);

	/**
	 * fetches the property file inside jar location.
	 * 
	 * @param propertyFile
	 * @return
	 */
	public static Properties getProperties(String propertyFile) {
		Properties properties = new Properties();
		InputStream inputStream = null;
		try {
			System.out.println("property file" + propertyFile);
			inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(propertyFile);
			if (inputStream != null) {
				properties.load(inputStream);
			} else {
				Properties.error("Property file Not Found");
			}
		} catch (Exception e) {
			Properties.error("Property file Not Found", e);
		} finally {
			try {
				inputStream.close();
			} catch (IOException e) {
				Properties.error("Cannot Close IO stream", e);
				e.printStackTrace();
			}
		}
		return properties;

	}

}
