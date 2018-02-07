package com.kstream.operations;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.log4j.Logger;

import com.kstream.util.Constants;
import com.kstream.util.serde.hashmap.HashMapSerde;
import com.kstream.util.serde.list.ArrayListSerde;

public class EntityOperation<K, V, T> {
	
	public static Logger logger = Logger.getLogger(EntityOperation.class);
	// VR, K1, KR, V1
	/**
	 * Group the records based on the input stream key
	 * 
	 * @param <K>
	 *            Type of key
	 * @param <V>
	 *            Type of value
	 * @param <T>
	 *            Type to be serialized from and deserialized into.
	 * @param streamObject
	 * @param serdeType
	 *            aggregate value serdes for materializing the aggregated table
	 * @param storageName
	 * @return a {@link KTable} that contains "update" records with modified
	 *         keys, and values that represent the latest aggregate for each key
	 */
	public KTable<K, ArrayList<V>> groupAndAggregate(KStream<K, V> streamObject, Serde<T> serdeType,
			String storageName) {
		KTable<K, ArrayList<V>> groupedTable = streamObject.groupByKey().aggregate(
				// Custom Initializer
				ArrayList::new,
				// aggregator
				(key, value, list) -> {
					list.add(value);
					return list;
				}, new ArrayListSerde(serdeType), storageName);
		return groupedTable;
	}

	/**
	 * Group the records based on input stream key and create a map with string
	 * label and list as values
	 * 
	 * @param <K>
	 *            Type of key
	 * @param <V>
	 *            Type of value
	 * @param <T>
	 *            Type to be serialized from and deserialized into.
	 * @param streamObject
	 * @param serdeType
	 *            aggregate value serdes for materializing the aggregated table
	 * @param storageName
	 * @param labelName
	 *            Label for AggregatedTable to convert into Map<label,list
	 *            <Object>>
	 * @return a {@link KTable} that contains "update" records with modified
	 *         keys, and values that represent the latest aggregate for each key
	 */
	public KTable<K, Map<String, ArrayList<V>>> groupAndAggregateLabel(KStream<K, V> streamObject, Serde<T> serdeType,
			String storageName, String labelName) {
		KTable<K, ArrayList<V>> groupedTable = streamObject.groupByKey().aggregate(
				// Custom Initializer
				ArrayList::new,
				// aggregator
				(key, value, list) -> {
					
					logger.info("---- groupAndAggregateLabel method : Key.----------------- ");
					logger.info(key);
					logger.info(key.getClass());
					
					list.add(value);
					
					logger.info(list);
					return list;
				}, new ArrayListSerde(serdeType), storageName);

		KTable<K, Map<String, ArrayList<V>>> labeledGroupedTable = groupedTable.mapValues(value -> {
			Map<String, ArrayList<V>> map = new HashMap<String, ArrayList<V>>();
			map.put(labelName, value);
			logger.info("//------------------ Map value-----------------//");
			logger.info(map);
			return map;
		});
		
		return labeledGroupedTable;
	}

	/**
	 * Group the records based on custom key
	 * 
	 * @param <K>
	 *            Type of key
	 * @param <V>
	 *            Type of value
	 * @param <T>
	 *            Type to be serialized from and deserialized into.
	 * @param <K1>
	 *            Type of Key used for grouping
	 * @param streamObject
	 * @param keySerdeType
	 *            Serde for key used for grouping
	 * @param valueSerdeType
	 *            aggregate value serdes for materializing the aggregated table
	 * @param storageName
	 * @param labelName
	 *            Label for AggregatedTable to convert into Map<label,list
	 *            <Object>>
	 * @return a {@link KTable} that contains "update" records with modified
	 *         keys, and values that represent the latest aggregate for each key
	 */
	public KTable<byte[], Map<String, ArrayList<V>>> groupByCustomKeyAndAggregateLabel(KStream<K, V> streamObject,
			String customKey, Serde<byte[]> keySerdeType, Serde<V> valueSerdeType, String storageName, String labelName) {

		KGroupedStream<byte[], V> kGroupedStreams = streamObject.groupBy((key, value) -> {
			Object customKeyValue = null;
			if (value instanceof GenericRecord) {
				GenericRecord value1 = (GenericRecord) value;
				customKeyValue = ((Utf8) value1.get(customKey));
			}
			if (value instanceof Map) {
				Map value1 = (Map) value;
				customKeyValue = ((Utf8) value1.get(customKey));
			}
			logger.info(customKeyValue);
			byte[] finalKey = null;
			finalKey =  customKeyValue.toString().getBytes();
		 //	finalKey = getDataTypeFromSerdeType(keySerdeType, customKeyValue);
			logger.info("//-----------------------groupByCustomKeyAndAggregateLabel method : FinalKey------------------//");
			logger.info(finalKey);
			logger.info(finalKey.getClass());
			return finalKey;
		}
		, Serialized.with(Serdes.ByteArray(), valueSerdeType)
				);
		KTable<byte[], ArrayList<V>> kGroupedTable = kGroupedStreams.aggregate(
				// initializer
				() -> new ArrayList<V>(), (customKeyValue, value, valueList) -> {
					valueList.add(value);
					logger.info(valueList);
					return valueList;
				}, new ArrayListSerde(valueSerdeType), storageName

		);
 
		KTable<byte[], Map<String, ArrayList<V>>> labeledGroupedTable = kGroupedTable.mapValues(value -> {
			Map<String, ArrayList<V>> map = new HashMap<String, ArrayList<V>>();
			map.put(labelName, value);
			logger.info(map);
			return map;
		});
		logger.info(labeledGroupedTable);
		return labeledGroupedTable;
	}

	public KTable<T, Map<String, ArrayList<V>>> groupByCustomKeyAndAggregateLabel2(KStream<K, V> streamObject,
			String customKey, Serde<T> keySerdeType, Serde<V> valueSerdeType, String storageName, String labelName) {

		KGroupedStream<T, V> kGroupedStreams = streamObject.groupBy((key, value) -> {
			Object customKeyValue = null;
			if (value instanceof GenericRecord) {
				GenericRecord value1 = (GenericRecord) value;
				customKeyValue = ((Utf8) value1.get(customKey));
			}
			if (value instanceof Map) {
				Map value1 = (Map) value;
				customKeyValue = ((Utf8) value1.get(customKey));
			}
			logger.info(customKeyValue);
			T finalKey = null;
		//	finalKey =  customKeyValue.toString().getBytes();
		 	finalKey = getDataTypeFromSerdeType(keySerdeType, customKeyValue);
			logger.info("//-----------------------groupByCustomKeyAndAggregateLabel method : FinalKey------------------//");
			logger.info(finalKey);
			logger.info(finalKey.getClass());
			return finalKey;
		}
		, Serialized.with(keySerdeType, valueSerdeType)
				);
		KTable<T, ArrayList<V>> kGroupedTable = kGroupedStreams.aggregate(
				// initializer
				() -> new ArrayList<V>(), (customKeyValue, value, valueList) -> {
					valueList.add(value);
					logger.info(valueList);
					return valueList;
				}, new ArrayListSerde(valueSerdeType), storageName

		);
 
		KTable<T, Map<String, ArrayList<V>>> labeledGroupedTable = kGroupedTable.mapValues(value -> {
			Map<String, ArrayList<V>> map = new HashMap<String, ArrayList<V>>();
			map.put(labelName, value);
			logger.info(map);
			return map;
		});
		logger.info(labeledGroupedTable);
		return labeledGroupedTable;
	}
	
	/**
	 * Join two Ktables
	 * 
	 * @param <K>
	 *            Type of primary key
	 * @param <V1>
	 *            Value type for first KTable
	 * @param <V2>
	 *            Value type for second KTable
	 * @param table1
	 *            first KTable
	 * @param table2
	 *            second KTable
	 * @param schema
	 *            Schema for merged Record
	 * @return a {@code KTable<K,GenericRecord} that contains join-records for
	 *         each key and values computed by the given {@link ValueJoiner},
	 *         one for each matched record-pair with the same key
	 */
	public <V1, V2> KTable<K, GenericRecord> join(KTable<K, V1> table1, KTable<K, V2> table2, String schema) {
		// writing data in GenericRecord format
		logger.info("//--------------Join operation ------------------//");
		logger.info("--------Join method logger-----------------");
		try{
	 
		final Schema avroSchema = new Schema.Parser().parse(schema);
		GenericRecord record = new GenericData.Record(avroSchema);
		KTable<K, GenericRecord> joinedKTable = table1.join(table2, new ValueJoiner<V1, V2, GenericRecord>() {
			@Override
			public GenericRecord apply(V1 value1, V2 value2) {
				logger.info("//--------------Join operation apply method------------------//");
				logger.info(value1);
				logger.info(value2);
				try {
					if (value1 instanceof Map<?, ?>) {
						for (Entry<?, ?> entry : ((Map<?, ?>) value1).entrySet()) {
							String key = (String) entry.getKey();
							Object entryValue = entry.getValue();
							record.put(key, entryValue);
						}
					}
					if (value2 instanceof Map<?, ?>) {
						for (Entry<?, ?> entry : ((Map<?, ?>) value2).entrySet()) {
							String key = (String) entry.getKey();
							Object entryValue = entry.getValue();
							record.put(key, entryValue);
						}
					}
					logger.info("//-------------------- Join record----------------//");
					logger.info(record);
					return record;
				} catch (Exception e) {
					e.printStackTrace();
					return null;
				}
			}
		});
		return joinedKTable;
		}catch(Exception e){
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Join two Ktables
	 * 
	 * @param <K>
	 *            Type of primary key
	 * @param <V1>
	 *            Value type for first KTable
	 * @param <V2>
	 *            Value type for second KTable
	 * @param table1
	 *            first KTable
	 * @param table2
	 *            second KTable
	 * @param schema
	 *            Schema for merged Record
	 * @return a {@code KTable<K,byte[]} that contains join-records for each key
	 *         and values computed by the given {@link ValueJoiner}, one for
	 *         each matched record-pair with the same key
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
				try {
					if (value1 instanceof Map<?, ?>) {
						for (Entry<?, ?> entry : ((Map<?, ?>) value1).entrySet()) {
							String key = (String) entry.getKey();
							Object entryValue = entry.getValue();
							record.put(key, entryValue);
						}
					}
					if (value2 instanceof Map<?, ?>) {
						for (Entry<?, ?> entry : ((Map<?, ?>) value2).entrySet()) {
							String key = (String) entry.getKey();
							Object entryValue = entry.getValue();
							record.put(key, entryValue);
						}
					}
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

	private <K1> K1 getDataTypeFromSerdeType(Serde<K1> serdeType, Object value) {
		K1 modifiedValue = null;
		String serdeTypeClass = serdeType.getClass().getSimpleName();
//		logger.info("//-------------Inside GetDatatypeFromSerdeType method------------");
//		logger.info(serdeTypeClass);
//		logger.info(value);
//		logger.info(value.getClass());
		switch (serdeTypeClass) {
		case "StringSerde":
	//		logger.info(" ------------------First case: String_-----------");
			modifiedValue = (K1) value.toString();
			break;
		case "ByteArraySerde":
			modifiedValue = (K1)value.toString().getBytes();
			break;
		default:
			modifiedValue = (K1) value;
			break;
		}
		return modifiedValue;
	}
}
