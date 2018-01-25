package com.kstream.util.serde.hashmap;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import com.kstream.util.serde.list.ArrayListDeserializer;
import com.kstream.util.serde.list.ArrayListSerializer;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

public class HashMapSerde<V> implements Serde<HashMap> {

	private final Serde<HashMap> inner;
	
	
	public HashMapSerde(Serde serde){
		inner = Serdes.serdeFrom(new HashMapSerializer(serde.serializer()),new HashMapDeserializer<>(serde.deserializer()));
	}
	
    public Serializer<HashMap> serializer() {
    	return inner.serializer();
    }

    public Deserializer<HashMap> deserializer() {
        return inner.deserializer();
    }

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
}
