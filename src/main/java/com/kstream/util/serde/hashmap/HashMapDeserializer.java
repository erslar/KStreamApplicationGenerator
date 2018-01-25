package com.kstream.util.serde.hashmap;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

public class HashMapDeserializer<K, V> implements Deserializer<HashMap> {
	private final Deserializer<HashMap> valueDeserializer;

	public HashMapDeserializer(final Deserializer<HashMap> valueDeserializer) {
		this.valueDeserializer = valueDeserializer;
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// do nothing
	}

	@Override
	public HashMap<K, V> deserialize(String topic, byte[] bytes) {
		if (bytes == null || bytes.length == 0) {
			return null;
		}
		HashMap<K, V> map = new HashMap<>();
		try {
			final ObjectInputStream dataInputStream = new ObjectInputStream(new ByteArrayInputStream(bytes));
			final int records = dataInputStream.readInt();
			map = (HashMap) dataInputStream.readObject();

		} catch (IOException | ClassNotFoundException e) {
			throw new RuntimeException("Unable to deserialize HashMap", e);
		}
		return map;
	}

	@Override
	public void close() {
		// do nothing
	}

}
