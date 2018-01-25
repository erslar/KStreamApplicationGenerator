package com.kstream.impl.registry;

import java.util.Properties;

public class SchemaRegistryReader {

	public static void main(String[] args){
		Properties props = new Properties();
		String url = "";
		props.put("schema.registry.url", url);
		props.put("key.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
		props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
	}
}
