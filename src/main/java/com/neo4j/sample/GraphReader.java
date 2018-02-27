package com.neo4j.sample;

import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.log4j.Logger;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;

import com.kstream.util.GraphConstants;

import scala.util.parsing.json.JSONObject;

public class GraphReader implements AutoCloseable {

	private final Driver driver;
	private static Logger logger = Logger.getLogger(GraphReader.class);
	static VelocityEngine ve = new VelocityEngine();
	static VelocityContext context;
	static StringWriter writer = new StringWriter();
	int counter = 0;
	int maxCounter = 5;
	static {
		ve.init();
	}

	public GraphReader(String uri, String user, String password) {
		driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
	}

	public static void main(String... args) throws Exception {
		String user = "neo4j";
		String password = "root";
		String uri = "bolt://localhost/";

		// GraphReader.testMethod();
		// String inputQuery = "MATCH (i:Input)-[rel*1]-(connected) return
		// i,connected,rel";
		// String inputQuery = "MATCH (i:Input{ name:'claim'}) RETURN i";
		String inputQuery = "MATCH (i:Input) RETURN i";

		try (GraphReader reader = new GraphReader(uri, user, password)) {
			reader.readGraph(inputQuery);
		}

	}

	/**
	 * Read the Graph from depth 0. Assuming Input is the starting node
	 * 
	 * @param query
	 */
	public void readGraph(String query) {
		try (Session session = driver.session()) {
			StatementResult result = session.run(query);
			List<String> resultKeyList = result.keys();
			logger.info("-------------------------");
			String keyType = StringUtils.EMPTY;

			if (result.keys().size() == 1) {
				keyType = resultKeyList.get(0);
			}
			List<Record> recordList = result.list();
			recordList.forEach(record -> {
				String nodeKeyType = StringUtils.EMPTY;
				if (result.keys().size() == 1) {
					nodeKeyType = resultKeyList.get(0);
				}
				Node node = record.get(nodeKeyType).asNode();
				Iterable<String> labels = node.labels();
				String nodeLabel = labels.iterator().next();
				long nodeId = node.id();
				logger.info(" ---------------------- Node name : " + record.get("i").get("name") + " , Node id : "+nodeId + ", nodeLabel :  "+ nodeLabel);

				// find all connected node at depth 1
				findConnectedNodes(nodeId, nodeLabel, 1, session);
			});

			// }
		}
	}

	/**
	 * Get all connected node for a given start node where startNode ID is
	 * passed as input to the query
	 * 
	 * @param nodeId
	 * @param nodeType
	 * @param depth
	 * @param session
	 */
	public void findConnectedNodes(long nodeId, String nodeType, int depth, Session session) {
		logger.info("------------------------entering into findConnectedNodes----------------------------------------");
		logger.info("Counter value : " +counter);
		
		while (counter < 10) {
			logger.info("------------------------inside findConnectedNodes----------------------------------------");
		
			logger.info("nodeId : " +nodeId + ", depth : " + depth + ",  Node type " + nodeType);
			String query = "MATCH (i :" + nodeType + ")-[rel*" + depth + "]-(connected) WHERE ID(i) = " + nodeId
					+ " RETURN i,connected,rel , count(connected) as degree  ";
			
			logger.info(query);
			// try (Session session = driver.session()) {
			StatementResult result = session.run(query);
			List<Record> records = result.list();
			logger.info(result);
			logger.info(records.size());
			logger.info(records.isEmpty());
			 
			if (!records.isEmpty()) {
				counter++;
				logger.info("----- records are not empty-----------------------------");
				for (Record record : records) {
					logger.info("........ Inside While loop for result---------------------------------");
					logger.info(record);
					long degree = record.get("degree").asLong();
					logger.info("--- Degree of Node : " + degree);		
					Map<String, Object> inputNodesProperties = record.get("i").asNode().asMap();

					logger.info(inputNodesProperties);
					Node connectedNodes = record.get("connected").asNode();
					long destNodeId = connectedNodes.id();
					String destNodeType = connectedNodes.labels().iterator().next();
					Map<String, Object> connectedNodeProperties = connectedNodes.asMap();

					String schema = inputNodesProperties.containsKey(GraphConstants.SCHEMA)
							? inputNodesProperties.get(GraphConstants.SCHEMA).toString() : StringUtils.EMPTY;

					logger.info("------------------  connected Nodes -------------------- ");
					logger.info(connectedNodeProperties); // find type of
															// relationships
					Iterator<Value> relIterator = record.get("rel").values().iterator();

					logger.info("------------------  Relationships  -------------------- ");
					while (relIterator.hasNext()) {
						Relationship relationObject = relIterator.next().asRelationship();
						long endNodeId = relationObject.endNodeId();

						String relType = relationObject.type();

						List<Node> startNodes = getStartNodes(relType, endNodeId, session);

						long startNodeId = relationObject.startNodeId();
						logger.info(relType);
						// Generate Schema for each connected Node
						generateSchema(startNodes, relType, connectedNodes, session);
						// generateSchema(schema, relType, connectedNodes,
						// session);
					}
					logger.info("------------------------------------- DestNode Id-------------------------");
					logger.info(destNodeId);
					// run recursive to iterate all nodes
					findConnectedNodes(destNodeId, destNodeType, depth, session);
					logger.info(
							"------------------------exiting findConnectedNodes----------------------------------------");
				}
			} else {
				break;
			}
		}
		// }
	}

	public static List<Node> getStartNodes(String relationType, long endNodeId, Session session) {
		try {
			String query = "MATCH (i)-[rel:" + relationType + "]-(connectedNode) where ID(connectedNode) = " + endNodeId
					+ " RETURN i,connectedNode, startNode(rel) as startNode, endNode(rel) as endNodes ";
			StatementResult result = session.run(query);
			List<Node> startNodes = new ArrayList<Node>();
			while (result.hasNext()) {
				Record record = result.next();
				Node startNode = record.get("startNode").asNode();
				startNodes.add(startNode);

			}
			logger.info(startNodes);
			return startNodes;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Generate the schema based on input schema and defined relation and assign
	 * to connected node schema attribute
	 * 
	 * @param inputSchema
	 * @param operation
	 * @param nodeProperties
	 */
	public void generateSchema(String inputSchema, String operation, Node node, Session session) {
		logger.info("-------------------- Generating Schema-----------------------------------");

		StringWriter writer = new StringWriter();
		String outputSchema = StringUtils.EMPTY;
		switch (operation.toLowerCase()) {
		case GraphConstants.GROUPBY:
			String outputLabel = node.asMap().get(GraphConstants.OUTPUT_LABEL).toString();
			long nodeId = node.id();
			logger.info(outputLabel);
			// load group by schema template
			Template template = loadTemplate(operation);
			context = new VelocityContext();
			context.put("schemaObjectName", outputLabel);
			context.put("sourceSchema", inputSchema);
			template.merge(context, writer);
			outputSchema = writer.toString();
			logger.info(outputSchema);

			if (!outputSchema.isEmpty()) {
				setProperty(nodeId, outputSchema, session, GraphConstants.SCHEMA);
			}
			break;
		case GraphConstants.JOIN:
			// load join schema template
			break;
		case GraphConstants.TRANSFORM:
			// load transform schema template
			break;
		default:
			break;
		}
		logger.info("----------------------- exiting GenerateSchema----------------------------------------");
	}

	public void generateSchema(List<Node> startNodes, String operation, Node endNode, Session session) {
		logger.info("-------------------- Generating Schema-----------------------------------");

		StringBuilder builder = new StringBuilder();
		logger.info(startNodes.size());
		Template template = loadTemplate(operation);
		context = new VelocityContext();

		StringWriter writer = new StringWriter();
		String outputSchema = StringUtils.EMPTY;
		String outputLabel = endNode.asMap().get(GraphConstants.OUTPUT_LABEL).toString();
		long nodeId = endNode.id();
		logger.info(outputLabel);
		context.put("schemaObjectName", outputLabel);

		String mergedSchema = startNodes.stream().map(node -> node.get(GraphConstants.SCHEMA).asString())
				.collect(Collectors.joining(","));

		switch (operation.toLowerCase()) {
		case GraphConstants.GROUPBY:
			// load group by schema template

			String inputSchema = mergedSchema;
			context.put("sourceSchema", inputSchema);

			break;
		case GraphConstants.JOIN:
			String joinType = endNode.get(GraphConstants.JOINTYPE).asString();
			context.put("sourceSchema", mergedSchema);

			// load join schema template
			break;
		case GraphConstants.TRANSFORM:
			// load transform schema template
			break;
		default:
			break;
		}
		template.merge(context, writer);
		outputSchema = writer.toString();
		logger.info(outputSchema);
		if (!outputSchema.isEmpty()) {
			setProperty(nodeId, outputSchema, session, GraphConstants.SCHEMA);
		}
		logger.info("----------------------- exiting GenerateSchema----------------------------------------");
	}

	private Template loadTemplate(String operation) {
		Template template = ve
				.getTemplate("src/main/resources/templates/avroSchema/" + operation + "SchemaTemplate.vm");
		return template;

	}

	private void setProperty(long nodeId, String schema, Session session, String property) {
		String escapedSchema = StringEscapeUtils.escapeJava(schema);
		String query = "MATCH (node) where ID(node) = " + nodeId + " set node." + property + " = \"" + escapedSchema
				+ " \" return node";
		logger.info(query);
		// executeQuery(session, query);

	}

	/**
	 * Execute query statement
	 * 
	 * @param session
	 * @param query
	 */
	private static void executeQuery(Session session, String query) {
		try {
			StatementResult result = session.run(query);
			logger.info(result);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() throws Exception {
		// TODO Auto-generated method stub
		driver.close();
	}
}
