package com.kstream.impl.registry;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.utils.VerifiableProperties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;

import com.fasterxml.jackson.databind.JsonNode;

import io.confluent.kafka.serializers.KafkaAvroDecoder;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.IntStream;

public class KafkaConsumer3 {
    
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("zookeeper.connect", "localhost:2181");
		props.put("group.id", "group1");
		props.put("auto.offset.reset", "earliest");
		props.put("schema.registry.url", "http://10.101.14.29:8081");
		props.put("bootstrap.servers", "cognnitKafkaBroker1:9092");
		props.put("key.deserializer", ByteArrayDeserializer.class);
		props.put("value.deserializer", ByteArrayDeserializer.class);

		String topic = "customer5";
		Collection<String> topics = new ArrayList<>();
		 topics.add("customer5");
		Map<String, Integer> topicCountMap = new HashMap<>();
		// topicCountMap.put(topics, new Integer(1));

//		VerifiableProperties vProps = new VerifiableProperties(props);
//		KafkaAvroDecoder keyDecoder = new KafkaAvroDecoder(vProps);
//		KafkaAvroDecoder valueDecoder = new KafkaAvroDecoder(vProps);

		KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
		JsonDeserializer deserializer = new JsonDeserializer();
		try {
			consumer.subscribe(topics);
			System.out.println("*=====================*");
			
			IntStream.range(1, 1000000).forEach(index -> {
				ConsumerRecords records1 = consumer.poll(10);
				System.out.println(records1);
				ConsumerRecords<byte[], byte[]> records = consumer.poll(10);
 
				for (ConsumerRecord<byte[],byte[]> record : records) {
					System.out.println(record);
					Map<String, Object> data = new HashMap<>();

					data.put("partition", record.partition());
					data.put("offset", record.offset());
					data.put("value", record.value());
				 	System.out.println(record.value());
				 	try{
				 	JsonNode jsonValue = deserializer.deserialize(topic, record.value());
				 	System.out.println(jsonValue);
				 	System.out.println(jsonValue.fieldNames());
				 	}catch(Exception e){
				 		e.printStackTrace();
				 	}
			//		System.out.println(new String(record.value()));
			//		System.out.println(record.key() + ": " + data);

		/*			Schema schema = null;
					schema = new Schema.Parser().parse(jsonSchema);
					 GenericRecord genericRecord = decodeMessage(schema, record.value());
					System.out.println("===================== Printing GenericRecord ====================");
					System.out.println(genericRecord);
					System.out.println(genericRecord.get("data").toString());
			*/		
				 	
				 
					 
			 	} 
			});
		} catch (Exception e) {
			e.printStackTrace();
			// ignore for shutdown
		} finally {
			// consumer.close();
		}

	}

	 
	
	
	 public static GenericRecord decodeMessage(Schema schema, byte[] byteData) {
	        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
	        ByteArrayInputStream byteArrayInputStream = null;
	        try {
	            byteArrayInputStream = new ByteArrayInputStream(byteData);
	            Decoder decoder = DecoderFactory.get().binaryDecoder(byteArrayInputStream, null);
	            return reader.read(null, decoder);
	        } catch (IOException e) {
	            return null;
	        } finally {
	            try {
	                byteArrayInputStream.close();
	            } catch (IOException e) {

	            }
	        }
	    }
	 
	 
	 public static GenericArray decodeMessage2(Schema schema, byte[] byteData) {
	        GenericDatumReader<GenericArray> reader = new GenericDatumReader<GenericArray>(schema);
	        ByteArrayInputStream byteArrayInputStream = null;
	        try {
	            byteArrayInputStream = new ByteArrayInputStream(byteData);
	            Decoder decoder = DecoderFactory.get().binaryDecoder(byteArrayInputStream, null);
	            return reader.read(null, decoder);
	        } catch (IOException e) {
	            return null;
	        } finally {
	            try {
	                byteArrayInputStream.close();
	            } catch (IOException e) {

	            }
	        }
	    }
}