package com.kstream.util.serde.hashmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.serialization.Serializer;

public class HashMapSerializer<K, V> implements Serializer<HashMap> {

	private Serializer<HashMap> inner;

	public HashMapSerializer(Serializer<HashMap> inner) {
		this.inner = inner;
	}

	// Default constructor needed by Kafka
	public HashMapSerializer() {
	}

	public byte[] serialize(String topic, HashMap  queue) {
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			
			// TODO . serialization fails.. Need to revisit the code
			// Current error : 
			Set<K> key = queue.keySet();
			Collection<V> value = queue.values();
			
			System.out.println(queue);
			final ObjectOutputStream dos = new ObjectOutputStream(baos);
			
			dos.writeObject(queue);
			System.out.println(dos);

		} catch (IOException e) {
			throw new RuntimeException("Unable to serialize HashMap", e);
		}
		return baos.toByteArray();
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
