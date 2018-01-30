package com.kstream.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kstream.operations.EntityOperation;
import com.kstream.util.Constants;
import com.kstream.util.PropertyUtil;
import com.kstream.util.StreamConfiguration;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

/**
 * Generic Pipeline to join two kafka topics
 * 
 * @author nishu.tayal
 *
 */
public class CustomerAnalysisApp {

	public static Logger logger = LoggerFactory.getLogger(CustomerAnalysisApp.class);

	public static void main(String[] args) {

		EntityOperation entityOperation = new EntityOperation<>();

		Properties customProperties = PropertyUtil.getProperties(Constants.PROPERTIES_FILE);

		String TOPIC1_LABEL = customProperties.getProperty("topic1Label");
		String TOPIC2_LABEL = customProperties.getProperty("topic2Label");

		String methodName = new Object() {
		}.getClass().getEnclosingMethod().getName();


		long timestamp = new Date().getTime();

		String firstTopic = customProperties.getProperty("topic1");
		String secondTopic = customProperties.getProperty("topic2");
	 
		String outputTopic = customProperties.getProperty("outputTopic");

		String firstStorage = String.format("%1$s_store_1_%2$s", methodName, timestamp);
		String secondStorage = String.format("%1$s_store_2_%2$s", methodName, timestamp);

		try {

			Properties streamsConfiguration = StreamConfiguration.getConf(Constants.PROPERTIES_FILE);

			Serializer kafkaAvroSerializer = new KafkaAvroSerializer();
			kafkaAvroSerializer.configure(streamsConfiguration, false);

			Deserializer kafkaAvroDeserializer = new KafkaAvroDeserializer();
			kafkaAvroDeserializer.configure(streamsConfiguration, false);

			Serde<GenericRecord> avroSerde = Serdes.serdeFrom(kafkaAvroSerializer, kafkaAvroDeserializer);

			Serde outputKeySerde = Serdes.ByteArray();
			// Read the kafka topics
			StreamsBuilder builder = new StreamsBuilder();

			KStream<byte[], GenericRecord> firstStream = builder.stream(firstTopic);
			KStream<byte[], GenericRecord> secondStream = builder.stream(secondTopic);

			KTable<byte[], Map<String, ArrayList<GenericRecord>>> firstTable = entityOperation
					.groupAndAggregateLabel(firstStream, avroSerde, firstStorage, TOPIC1_LABEL);
			KTable<byte[], Map<String, ArrayList<GenericRecord>>> secondTable = entityOperation
					.groupAndAggregateLabel(secondStream, avroSerde, secondStorage, TOPIC2_LABEL);

			// writing data in GenericRecord format
			KTable<byte[], GenericRecord> joinedKTable = entityOperation.join(firstTable, secondTable,
					Constants.OUTPUT_SCHEMA1);

			joinedKTable.toStream().to(outputTopic, Produced.with(outputKeySerde, avroSerde));

			KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
			streams.cleanUp();
			streams.start();
			// streams.close();

		} finally {

			// RestUtils.deleteTopics(firstTopic, secondTopic, outputTopic);
		}

	}
}
