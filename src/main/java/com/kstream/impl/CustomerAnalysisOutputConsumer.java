package com.kstream.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.IntStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;

import com.kstream.util.Constants;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

/**
 * 
 * @author nishu
 *
 */
public class CustomerAnalysisOutputConsumer {

	public static void main(String[] args) {
	//	consumeByteArray();;
	 	consumeAvroMessage();

	}

	static void consumeByteArray() {
		Properties props = new Properties();
		props.put("zookeeper.connect", "localhost:2181");
		props.put("group.id", "group1");
		props.put("auto.offset.reset", "earliest");
		props.put("schema.registry.url", "http://10.101.14.29:8081");
		props.put("bootstrap.servers", "cognnitKafkaBroker1:9092");
		props.put("key.deserializer", ByteArrayDeserializer.class);
		props.put("value.deserializer", ByteArrayDeserializer.class);

		String topic = "customerPolicy15";
		Collection<String> topics = new ArrayList<>();
		topics.add(topic);
		Map<String, Integer> topicCountMap = new HashMap<>();

		Schema custSchema = new Schema.Parser().parse(Constants.OUTPUT_SCHEMA1);
		
		DatumReader<GenericRecord> reader = new GenericDatumReader<>(custSchema);
		GenericRecord custViewRec = new GenericData.Record(custSchema);

		KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
		JsonDeserializer deserializer = new JsonDeserializer();
		try {
			consumer.subscribe(topics);

			IntStream.range(1, 1000000).forEach(index -> {

				ConsumerRecords<byte[], byte[]> records = consumer.poll(10);
				System.out.println(records);
				for (ConsumerRecord<byte[], byte[]> record : records) {
					System.out.println(record);
					byte[] custRec = record.value();
					System.out.println(custRec);
					Decoder decoder = DecoderFactory.get().binaryDecoder(custRec, null);
					try {
						GenericRecord payload = reader.read(null,decoder);
						System.out.println("//--------------- Here is the Payload message-------------//");
						System.out.println(payload);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					System.out.println("//--------------------- Customer Record ------------------");
					System.out.println(custRec);
					//System.out.println(custRec.getSchema());
					//System.out.println(custRec.get("customerRecords"));
				}
			});
		} catch (SerializationException e) {
			e.printStackTrace();
			// may need to do something with it
		}
	}

	// Using KafkaAvroDeserializer to deserializer avro GenericRecord
	static void consumeAvroMessage() {
		Properties props = new Properties();
		props.put("zookeeper.connect", "localhost:2181");
		props.put("group.id", "group2");
		props.put("auto.offset.reset", "earliest");
		props.put("schema.registry.url", "http://10.101.14.29:8081");
		props.put("bootstrap.servers", "cognnitKafkaBroker1:9092");
		props.put("key.deserializer", ByteArrayDeserializer.class);
		props.put("value.deserializer", KafkaAvroDeserializer.class);

		String topic = "customerPolicy15";
		Collection<String> topics = new ArrayList<>();
		topics.add(topic);
		Map<String, Integer> topicCountMap = new HashMap<>();

	

		KafkaConsumer<byte[], GenericRecord> consumer = new KafkaConsumer<>(props);
		JsonDeserializer deserializer = new JsonDeserializer();
		try {
			consumer.subscribe(topics);

			IntStream.range(1, 1000000).forEach(index -> {

				ConsumerRecords<byte[], GenericRecord> records = consumer.poll(10);
				System.out.println(records);
				for (ConsumerRecord<byte[], GenericRecord> record : records) {
					System.out.println(record);
					System.out.println(record.value());
					GenericRecord custRec = record.value();
					System.out.println("//--------------------- Customer Record ------------------");
					System.out.println(custRec);
					System.out.println(custRec.getSchema());
					System.out.println(custRec.get("customerRecords"));
				}
			});
		} catch (SerializationException e) {
			e.printStackTrace();
			// may need to do something with it
		}
	}

}
