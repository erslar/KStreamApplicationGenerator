package com.kstream.operations;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;

import com.kstream.util.Constants;
import com.kstream.util.serde.hashmap.HashMapSerde;
import com.kstream.util.serde.list.ArrayListSerde;

public class EntityOperation<K, V, T, VR> {

	public KTable<K, ArrayList<V>> groupAndAggregate(KStream<K, V> streamObject, Serde<T> serdeType,
			String storageName) {
		KTable<K, ArrayList<V>> groupedTable = streamObject.groupByKey().aggregate(
				// Custom Initializer
				ArrayList::new,
				// aggregator
				(k, v, list) -> {
					list.add(v);
					return list;
				}, new ArrayListSerde(serdeType), storageName);
		return groupedTable;
	}

	public KTable<K, Map<String, ArrayList<V>>> groupAndAggregateLabel(KStream<K, V> streamObject, Serde<T> serdeType,
			String storageName, String labelName) {
		KTable<K, ArrayList<V>> groupedTable = streamObject.groupByKey().aggregate(
				// Custom Initializer
				ArrayList::new,
				// aggregator
				(k, v, list) -> {
					list.add(v);
					return list;
				}, new ArrayListSerde(serdeType), storageName);

		KTable<K, Map<String, ArrayList<V>>> labledGroupedTable = groupedTable.mapValues(value -> {
			Map<String, ArrayList<V>> map = new HashMap<String, ArrayList<V>>();
			map.put(labelName, value);
			return map;
		});
		return labledGroupedTable;
	}

	/**
	 * Join two Ktables
	 * 
	 * @param table1
	 * @param table2
	 * @param schema
	 * @return KTable<K,GenericRecord>
	 */
	public <V1, V2> KTable<K, GenericRecord> join(KTable<K, V1> table1, KTable<K, V2> table2, String schema) {
		// writing data in GenericRecord format
		final Schema avroSchema = new Schema.Parser().parse(schema);
		GenericRecord record = new GenericData.Record(avroSchema);
		KTable<K, GenericRecord> joinedKTable = table1.join(table2, new ValueJoiner<V1, V2, GenericRecord>() {
			@Override
			public GenericRecord apply(V1 value1, V2 value2) {
				// TODO Auto-generated method stub
				System.out.println("//----------------------- Inside Join apply method---------------//");
				System.out.println("//--- Printing Value1 and Value2-------//");
				System.out.println(value1);
				System.out.println(value2);
				try {
					if (value1 instanceof Map<?, ?>) {
						for (Entry<?, ?> entry : ((Map<?, ?>) value1).entrySet()) {
							System.out.println(entry);
							String key = (String) entry.getKey();
							Object entryValue = entry.getValue();
							System.out.println(key);
							System.out.println(entryValue);
							record.put(key, entryValue);
						}
					}
					if (value2 instanceof Map<?, ?>) {
						System.out.println("//----------------- Inside value2 if statement-----------//");
						System.out.println(value2);
						System.out.println("//------------------- above is the value----------------//");
						for (Entry<?, ?> entry : ((Map<?, ?>) value2).entrySet()) {
							System.out.println(entry);
							String key = (String) entry.getKey();
							Object entryValue = entry.getValue();
							System.out.println(key);
							System.out.println(entryValue);
							record.put(key, entryValue);
						}
					}
					System.out.println("//--------------- Here is the Record---------//");
					System.out.println(record);
					return record;
				} catch (Exception e) {
					e.printStackTrace();
					return null;
				}

			}

		});
		return joinedKTable;
	}

	/**
	 * Join two Ktables
	 * 
	 * @param table1
	 * @param table2
	 * @param schema
	 * @return KTable<K,byte[]>
	 */
	public <V1, V2> KTable<K, byte[]> join2(KTable<K, V1> table1, KTable<K, V2> table2, String schema) {
		// writing data in GenericRecord format

		final Schema avroSchema = new Schema.Parser().parse(schema);
		GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(avroSchema);
		final EncoderFactory encoderFactory = EncoderFactory.get();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		BinaryEncoder encoder = encoderFactory.binaryEncoder(out, null);
		GenericRecord record = new GenericData.Record(avroSchema);
		KTable<K, byte[]> joinedKTable = table1.join(table2, new ValueJoiner<V1, V2, byte[]>() {
			@Override
			public byte[] apply(V1 value1, V2 value2) {
				// TODO Auto-generated method stub
				System.out.println("//----------------------- Inside Join apply method---------------//");

				System.out.println("//--- Printing Value1 and Value2-------//");
				System.out.println(value1);
				System.out.println(value2);
				try {
					if (value1 instanceof Map<?, ?>) {
						for (Entry<?, ?> entry : ((Map<?, ?>) value1).entrySet()) {
							System.out.println(entry);
							String key = (String) entry.getKey();
							Object entryValue = entry.getValue();
							System.out.println(key);
							System.out.println(entryValue);
							// ArrayList<GenericRecord> list = value1.get(key);
							record.put(key, entryValue);
						}
					}
					if (value2 instanceof Map<?, ?>) {
						System.out.println("//----------------- Inside value2 if statement-----------//");
						System.out.println(value2);
						System.out.println("//------------------- above is the value----------------//");
						for (Entry<?, ?> entry : ((Map<?, ?>) value2).entrySet()) {
							System.out.println(entry);
							String key = (String) entry.getKey();
							Object entryValue = entry.getValue();
							System.out.println(key);
							System.out.println(entryValue);
							// ArrayList<GenericRecord> list = value1.get(key);
							record.put(key, entryValue);
						}
					}
					System.out.println("//--------------- Here is the Record---------//");
					System.out.println(record);
					// Convert GenericRecord into bytes
					writer.write(record, encoder);
					encoder.flush();
					byte[] outBytes = out.toByteArray();
					return outBytes;
				} catch (Exception e) {
					e.printStackTrace();
					return null;
				}

			}

		});
		return joinedKTable;
	}
}

