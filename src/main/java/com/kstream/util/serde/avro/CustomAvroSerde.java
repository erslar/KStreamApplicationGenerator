package com.kstream.util.serde.avro;

import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class CustomAvroSerde implements Serde<GenericRecord> {
	
	private CustomAvroSerializer serializer = new CustomAvroSerializer();
	private CustomAvroDeserializer deserializer = new CustomAvroDeserializer();

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
		serializer.configure(configs, isKey);
		deserializer.configure(configs, isKey);
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		serializer.close();
		deserializer.close();
		
	}

	@Override
	public Serializer<GenericRecord> serializer() {
		// TODO Auto-generated method stub
		return serializer;
	}

	@Override
	public Deserializer<GenericRecord> deserializer() {
		// TODO Auto-generated method stub
		return deserializer;
	}

}
