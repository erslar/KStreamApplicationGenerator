package com.kstream.impl;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.ValueJoiner;

import com.kstream.util.Constants;
import com.kstream.util.PropertyUtil;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class CoreJoinExample {

	public static void main(String[] args) {

		Properties customProperties = PropertyUtil.getProperties(Constants.PROPERTIES_FILE);
		String BOOTSTRAP_SERVERS_CONFIG = customProperties.getProperty("bootstrap.servers");
		String ZOOKEEPER_CONNECT_CONFIG = customProperties.getProperty("zookeeper.url");
		String SCHEMA_REGISTRY_URL_CONFIG = customProperties.getProperty("schema.registry.url");

		String methodName = new Object() {
		}.getClass().getEnclosingMethod().getName();

		System.out.println(methodName);

		long timestamp = new Date().getTime();

		String firstTopic = String.format("%1$s_1_%2$s", methodName, timestamp);
		String secondTopic = String.format("%1$s_2_%2$s", methodName, timestamp);

		System.out.println(firstTopic);
		System.out.println(secondTopic);

		String outputTopic = String.format("%1$s_output_%2$s", methodName, timestamp);

		String firstStorage = String.format("%1$s_store_1_%2$s", methodName, timestamp);
		String secondStorage = String.format("%1$s_store_2_%2$s", methodName, timestamp);

		String appIdConfig = String.format("%1$s_app_id_%2$s", methodName, timestamp);
		String groupIdConfig = String.format("%1$s_group_id_%2$s", methodName, timestamp);

		String schemaIdNamespace = String.format("%1$s_id_ns_%2$s", methodName, timestamp);
		String schemaNameNamespace = String.format("%1$s_name_ns_%2$s", methodName, timestamp);
		String schemaScopeNamespace = String.format("%1$s_scope_ns_%2$s", methodName, timestamp);
		String schemaProjectionNamespace = String.format("%1$s_proj_ns_%2$s", methodName, timestamp);

		String schemaIdRecord = String.format("%1$s_id_rec_%2$s", methodName, timestamp);
		String schemaNameRecord = String.format("%1$s_name_rec_%2$s", methodName, timestamp);
		String schemaScopeRecord = String.format("%1$s_scope_rec_%2$s", methodName, timestamp);
		String schemaProjectionRecord = String.format("%1$s_proj_rec_%2$s", methodName, timestamp);

		
		
		
		try {
			Integer partitions = 1;
			Integer replication = 1;
			Properties topicConfig = new Properties();

			/*
			 * RestUtils.createTopic(firstTopic, partitions, replication,
			 * topicConfig); RestUtils.createTopic(secondTopic, partitions,
			 * replication, topicConfig); RestUtils.createTopic(outputTopic,
			 * partitions, replication, topicConfig);
			 */
			Properties streamsConfiguration = new Properties();
			streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appIdConfig);
			streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
			streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, ZOOKEEPER_CONNECT_CONFIG);
			streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
			streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
			streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
			streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/"); // TestUtils.tempDirectory().getAbsolutePath());
			streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
					SCHEMA_REGISTRY_URL_CONFIG);

			Serializer kafkaAvroSerializer = new KafkaAvroSerializer();
			kafkaAvroSerializer.configure(streamsConfiguration, false);

			Deserializer kafkaAvroDeserializer = new KafkaAvroDeserializer();
			kafkaAvroDeserializer.configure(streamsConfiguration, false);

			Serde<GenericRecord> avroSerde = Serdes.serdeFrom(kafkaAvroSerializer, kafkaAvroDeserializer);

			// -----

			Schema idSchema = SchemaBuilder.record(schemaIdRecord).namespace(schemaIdNamespace).fields().name("Id")
					.type().nullable().intType().noDefault().endRecord();
			
			System.out.println(idSchema);

			Schema nameSchema = SchemaBuilder.record(schemaNameRecord).namespace(schemaNameNamespace).fields()
					.name("Id").type().nullable().intType().noDefault().name("Name").type().nullable().stringType()
					.noDefault().endRecord();

			System.out.println(nameSchema);
			Schema scopeSchema = SchemaBuilder.record(schemaScopeRecord).namespace(schemaScopeNamespace).fields()
					.name("Scope").type().nullable().stringType().noDefault().endRecord();

			System.out.println(scopeSchema);
			Schema projectionSchema = SchemaBuilder.record(schemaProjectionRecord).namespace(schemaProjectionNamespace)
					.fields().name("Id").type().nullable().intType().noDefault().name("Name").type().nullable()
					.stringType().noDefault().name("Scope").type().nullable().stringType().noDefault().endRecord();

			System.out.println(projectionSchema);
			GenericRecord idRecord1 = new GenericData.Record(idSchema);
			idRecord1.put("Id", 1);
			GenericRecord idRecord2 = new GenericData.Record(idSchema);
			idRecord2.put("Id", 2);
			GenericRecord idRecord3 = new GenericData.Record(idSchema);
			idRecord3.put("Id", 3);
			GenericRecord idRecord4 = new GenericData.Record(idSchema);
			idRecord4.put("Id", 4);

			GenericRecord nameRecord1 = new GenericData.Record(nameSchema);
			nameRecord1.put("Id", 1);
			nameRecord1.put("Name", "Bruce Eckel");
			GenericRecord nameRecord2 = new GenericData.Record(nameSchema);
			nameRecord2.put("Id", 2);
			nameRecord2.put("Name", "Robert Lafore");
			GenericRecord nameRecord3 = new GenericData.Record(nameSchema);
			nameRecord3.put("Id", 3);
			nameRecord3.put("Name", "Andrew Tanenbaum");
			GenericRecord nameRecord4 = new GenericData.Record(nameSchema);
			nameRecord4.put("Id", 4);
			nameRecord4.put("Name", "Programming in Scala");

			GenericRecord scopeRecord1 = new GenericData.Record(scopeSchema);
			scopeRecord1.put("Scope", "Modern Operating System");
			GenericRecord scopeRecord2 = new GenericData.Record(scopeSchema);
			scopeRecord2.put("Scope", "Thinking in Java");
			GenericRecord scopeRecord3 = new GenericData.Record(scopeSchema);
			scopeRecord3.put("Scope", "Computer Architecture");
			GenericRecord scopeRecord4 = new GenericData.Record(scopeSchema);
			scopeRecord4.put("Scope", "Programming in Scala");

			List<KeyValue<GenericRecord, GenericRecord>> list1 = Arrays.asList(new KeyValue<>(idRecord1, nameRecord1),
					new KeyValue<>(idRecord2, nameRecord2), new KeyValue<>(idRecord3, nameRecord3));

			List<KeyValue<GenericRecord, GenericRecord>> list2 = Arrays.asList(new KeyValue<>(idRecord3, scopeRecord1),
					new KeyValue<>(idRecord1, scopeRecord2), new KeyValue<>(idRecord3, scopeRecord3),
					new KeyValue<>(idRecord4, scopeRecord4));

			GenericRecord projectionRecord1 = new GenericData.Record(projectionSchema);
			projectionRecord1.put("Id", nameRecord1.get("Id"));
			projectionRecord1.put("Name", nameRecord1.get("Name"));
			projectionRecord1.put("Scope", scopeRecord1.get("Scope"));

			GenericRecord projectionRecord2 = new GenericData.Record(projectionSchema);
			projectionRecord2.put("Id", nameRecord2.get("Id"));
			projectionRecord2.put("Name", nameRecord2.get("Name"));
			projectionRecord2.put("Scope", scopeRecord2.get("Scope"));

			GenericRecord projectionRecord3 = new GenericData.Record(projectionSchema);
			projectionRecord3.put("Id", nameRecord3.get("Id"));
			projectionRecord3.put("Name", nameRecord3.get("Name"));
			projectionRecord3.put("Scope", scopeRecord3.get("Scope"));

			List<KeyValue<GenericRecord, GenericRecord>> expectedResults = Arrays.asList(
					new KeyValue<>(idRecord3, projectionRecord3), new KeyValue<>(idRecord1, projectionRecord1),
					new KeyValue<>(idRecord3, projectionRecord3));

			// -----

			KStreamBuilder builder = new KStreamBuilder();

			KStream<GenericRecord, GenericRecord> firstStream = builder.stream(avroSerde, avroSerde, firstTopic);

			KStream<GenericRecord, GenericRecord> secondStream = builder.stream(avroSerde, avroSerde, secondTopic);

			KStream<GenericRecord, GenericRecord> outputStream = firstStream.join(secondStream,
					new ValueJoiner<GenericRecord, GenericRecord, GenericRecord>() {
						@Override
						public GenericRecord apply(GenericRecord l, GenericRecord r) {
							GenericRecord projectionRecord = new GenericData.Record(projectionSchema);
							projectionRecord.put("Id", l.get("Id"));
							projectionRecord.put("Name", l.get("Name"));
							projectionRecord.put("Scope", r.get("Scope"));
							return projectionRecord;
						}
					}, JoinWindows.of(TimeUnit.SECONDS.toMillis(5)), avroSerde, avroSerde, avroSerde);

			outputStream.to(avroSerde, avroSerde, outputTopic);

			KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);

			streams.start();

		/*	Properties cfg1 = new Properties();
			cfg1.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
			cfg1.put(ProducerConfig.ACKS_CONFIG, "all");
			cfg1.put(ProducerConfig.RETRIES_CONFIG, 0);
			cfg1.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
			cfg1.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
			cfg1.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL_CONFIG);
			IntegrationTestUtils.produceKeyValuesSynchronously(firstTopic, list1, cfg1);

			Properties cfg2 = new Properties();
			cfg2.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
			cfg2.put(ProducerConfig.ACKS_CONFIG, "all");
			cfg2.put(ProducerConfig.RETRIES_CONFIG, 0);
			cfg2.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
			cfg2.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
			cfg2.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL_CONFIG);
			IntegrationTestUtils.produceKeyValuesSynchronously(secondTopic, list2, cfg2);

			Properties consumerConfig = new Properties();
			consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
			consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupIdConfig);
			consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
			consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
			consumerConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL_CONFIG);

			List<KeyValue<GenericRecord, GenericRecord>> actualResults = IntegrationTestUtils
					.waitUntilMinKeyValueRecordsReceived(consumerConfig, outputTopic, expectedResults.size());
*/
			streams.close();

			// -----

			// -----
		} finally {

			// RestUtils.deleteTopics(firstTopic, secondTopic, outputTopic);
		}

	}
}
