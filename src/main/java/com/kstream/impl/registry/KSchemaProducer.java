package com.kstream.impl.registry;

import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import com.kstream.util.Constants;
import com.kstream.util.PropertyUtil;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class KSchemaProducer {

	public static void main(String[] args) {

		Properties customProperties = PropertyUtil.getProperties(Constants.PROPERTIES_FILE);
		String BOOTSTRAP_SERVERS_CONFIG = customProperties.getProperty("bootstrap.servers");
		String ZOOKEEPER_CONNECT_CONFIG = customProperties.getProperty("zookeeper.url");
		String SCHEMA_REGISTRY_URL_CONFIG = customProperties.getProperty("schema.registry.url");

		String topic1 = customProperties.getProperty("topic1");
		String topic2 = customProperties.getProperty("topic2");
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
	//	props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
		props.put("schema.registry.url", "http://10.101.14.29:8081");
		KafkaProducer producer = new KafkaProducer(props);

		String key = "2";
		String userSchema = "{\"type\":\"record\"," + "\"name\":\"customer\","
				+ "\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"address\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"string\"}]}";
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(userSchema);
		GenericRecord avroRecord = new GenericData.Record(schema);
		avroRecord.put("id", "2");
		avroRecord.put("address", "USA");
		avroRecord.put("timestamp", "46");

		ProducerRecord<Object, Object> record = new ProducerRecord<>(topic1, key , avroRecord);

		String key2 = "4";
		String policySchema = "{\"type\":\"record\"," + "\"name\":\"policy\","
				+ "\"fields\":[{\"name\":\"policy\",\"type\":\"string\"},{\"name\":\"policystarttime\",\"type\":\"string\"},{\"name\":\"policyendtime\",\"type\":\"string\"},{\"name\":\"pvar0\",\"type\":\"string\"},{\"name\":\"pvar1\",\"type\":\"string\"}]}";

		Schema schema2 = parser.parse(policySchema);
		GenericRecord avroRecord1 = new GenericData.Record(schema2);
		avroRecord1.put("policy", "4");
		avroRecord1.put("policystarttime", "15");
		avroRecord1.put("policyendtime", "71");
		avroRecord1.put("pvar0", "4");
		avroRecord1.put("pvar1", "9");
		
	 	ProducerRecord<Object, Object> record2 = new ProducerRecord<>(topic2, key2, avroRecord1);
 	 	
		try {
	 	   	producer.send(record2);
	 		producer.send(record);
	 		 
		} catch (Exception e) {
			e.printStackTrace();
			// may need to do something with it
		}
	}
}
