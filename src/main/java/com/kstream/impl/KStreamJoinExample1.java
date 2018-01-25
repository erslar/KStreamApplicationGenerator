package com.kstream.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kstream.util.Constants;
import com.kstream.util.PropertyUtil;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;

public class KStreamJoinExample1 {

	public static Logger logger = LoggerFactory.getLogger(KStreamJoinExample1.class);

	public static void main(String[] args) {

		Properties customProperties = PropertyUtil.getProperties(Constants.PROPERTIES_FILE);
		String BOOTSTRAP_SERVERS_CONFIG = customProperties.getProperty("bootstrap.servers");
		String ZOOKEEPER_CONNECT_CONFIG = customProperties.getProperty("zookeeper.url");
		String SCHEMA_REGISTRY_URL_CONFIG = customProperties.getProperty("schema.registry.url");

		String methodName = new Object() {
		}.getClass().getEnclosingMethod().getName();

		System.out.println(methodName);

		long timestamp = new Date().getTime();

		String firstTopic = customProperties.getProperty("topic1");
		String secondTopic = customProperties.getProperty("topic2");

		System.out.println(firstTopic);
		System.out.println(secondTopic);

		String outputTopic = customProperties.getProperty("outputTopic");

		String firstStorage = String.format("%1$s_store_1_%2$s", methodName, timestamp);
		String secondStorage = String.format("%1$s_store_2_%2$s", methodName, timestamp);

		String appIdConfig = String.format("%1$s_app_id_%2$s", methodName, timestamp);
		String groupIdConfig = String.format("%1$s_group_id_%2$s", methodName, timestamp);

		try {

			Properties streamsConfiguration = new Properties();
			streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appIdConfig);
			streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
			streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, ZOOKEEPER_CONNECT_CONFIG);
			streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
		 //	streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
		 	streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
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

			// Read the kafka topics

			StreamsBuilder builder = new StreamsBuilder();

			KStream<byte[], GenericRecord> firstStream = builder.stream(firstTopic);
			KStream<byte[], GenericRecord> secondStream = builder.stream(secondTopic);
			System.out.println(firstStream);
			System.out.println(secondStream);
			// KStream<GenericRecord, GenericRecord> secondStream =
			// builder.stream(avroSerde, avroSerde, secondTopic);

			KTable<byte[], GenericRecord> firstTable = firstStream.groupByKey()
					.reduce(new Reducer<GenericRecord>() {
						@Override
						public GenericRecord apply(GenericRecord aggValue, GenericRecord newValue) {
							System.out.println(newValue);
							return newValue;
						}
					}, "dummy-aggregation-store1_1");

			KTable<byte[], GenericRecord> secondTable = secondStream.groupByKey()
					.reduce(new Reducer<GenericRecord>() {
						@Override
						public GenericRecord apply(GenericRecord aggValue, GenericRecord newValue) {
						 
							System.out.println(newValue);
							return newValue;
						}
					}, "dummy-aggregation-store2_1");

		/*	KTable<byte[], GenericRecord> joinedKTable = firstTable.join(secondTable,
					(value1, value2) -> value2);
					*/
		//	System.out.println(joinedKTable);
		//	logger.info(joinedKTable.toString());

			String schema = "{\"namespace\":\"com.test\",\"name\":\"customerView\",\"type\":\"array\",\"items\":{\"name\":\"customer\",\"type\":\"record\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}}";
			final Schema avroSchema = new Schema.Parser().parse(schema);

			 final EncoderFactory encoderFactory = EncoderFactory.get();
			
			KTable<byte[], byte[]> joinedKTable2 = firstTable.join(secondTable,
					new ValueJoiner<GenericRecord, GenericRecord, byte[]>() {
						@Override
						public byte[] apply(GenericRecord l, GenericRecord r) {
							 System.out.println(l);
							 System.out.println(r);
							GenericArray<GenericRecord> avroArray = new GenericData.Array<GenericRecord>(10,
									avroSchema);
							avroArray.add(l);
							avroArray.add(r);
						//	final byte[] dt1 = System.out.println(avroArray.getSchema());

							GenericDatumWriter<GenericArray> writer = new GenericDatumWriter<>(avroSchema);
							ByteArrayOutputStream out = new ByteArrayOutputStream();
							Encoder binaryEncoder = encoderFactory.binaryEncoder(out, null);
							try {
								writer.write(avroArray, binaryEncoder);
								binaryEncoder.flush();
								out.close();
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						 
							System.out.println(out.toByteArray());
							return out.toByteArray();

						//	System.out.println(avroArray);
							// System.out.println(avroArray);
							 
						}
					});

		/*	KTable<GenericRecord, GenericArray<GenericRecord>> joinedKTable3 = firstTable.join(secondTable,
					new ValueJoiner<GenericRecord, GenericRecord, GenericArray<GenericRecord>>() {
						@Override
						public GenericArray<GenericRecord> apply(GenericRecord l, GenericRecord r) {
							GenericArray<GenericRecord> avroArray = new GenericData.Array<GenericRecord>(10,
									avroSchema);
							avroArray.add(l);
							avroArray.add(r);
							// System.out.println(avroArray);
							return avroArray;
						}
					});
					*/

			// (customer, policy) -> new CustomerAndPolicy(customer, policy));

			/*
			 * KStream<GenericRecord, List<GenericRecord>> outputStream =
			 * firstStream.join(secondStream, new ValueJoiner<GenericRecord,
			 * GenericRecord, List<GenericRecord>>() {
			 * 
			 * @Override public List<GenericRecord> apply(GenericRecord l,
			 * GenericRecord r) { List<GenericRecord> gList = new
			 * ArrayList<GenericRecord>(); gList.add(l); gList.add(r); return
			 * gList; } }, JoinWindows.of(TimeUnit.SECONDS.toMillis(5)),
			 * avroSerde, avroSerde, avroSerde);
			 * 
			 */
		//	joinedKTable2.toStream().to(outputTopic,Produced.with(Serdes.ByteArray(), Serdes.ByteArray()));

			KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
			streams.cleanUp();
			streams.start();

			// streams.close();

		} finally {

			// RestUtils.deleteTopics(firstTopic, secondTopic, outputTopic);
		}

	}
}
