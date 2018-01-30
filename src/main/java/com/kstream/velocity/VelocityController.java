package com.kstream.velocity;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kstream.util.Constants;
import com.kstream.util.PropertyUtil;

/**
 * Controller to run the velocity engine to generate the KStream applications
 * @author cognnit
 *
 */
public class VelocityController {

	public static Logger logger = LoggerFactory.getLogger(VelocityController.class);

	public static void main(String[] args) throws IOException {
		String propertyFile = Constants.PIPELINE_PROPERTY_FILE;
		String customPropertyFile = Constants.CUSTOM_PROPERTY_FILE;
		String outputDir = "src/main/java/velocity/templates/";
		Properties properties = PropertyUtil.getProperties(propertyFile);

		Map<String, String> propertyMap = new HashMap();

		propertyMap.putAll(properties.entrySet().stream()
				.collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString())));

		System.out.println(properties);

		String className = StringUtils.capitalize(properties.getProperty("className"));
		String packageName = properties.getProperty("packageName");
		String outputTopic = properties.getProperty("outputTopic");
		
		VelocityEngine ve = new VelocityEngine();
		ve.init();
		Template t = ve.getTemplate("src/main/resources/templates/KStreamTemplate1.vm");
		VelocityContext context = new VelocityContext();
		context.put("propertyMap", propertyMap);
		context.put("className", className);
		context.put("customPropertyFile", customPropertyFile);
		context.put("templatePropertyFile", propertyFile);
		context.put("packageName", packageName);
		context.put("outputTopic", outputTopic);
		context.put("topicNumbers", 2);
		String class1Path= outputDir+className + ".java";
		merge(t,context,class1Path);

	}

	private static void merge(Template template, VelocityContext context, String path) {
		PrintWriter writer = null;
		try {
			writer = new PrintWriter(path);
			template.merge(context, writer);
			writer.flush();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
