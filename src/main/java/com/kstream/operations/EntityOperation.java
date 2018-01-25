package com.kstream.operations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;

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
			Map<String,ArrayList<V>> map = new HashMap<String,ArrayList<V>>();
			map.put(labelName, value);
			return map;
		});
		return labledGroupedTable;
	}

	public <VO, VR> KStream<K, VR> join(final KStream<K, VO> otherStream,
			final ValueJoiner<? super V, ? super VO, ? extends VR> joiner, final JoinWindows windows,
			final Joined<K, V, VO> joined) {
		return null;
	}

}
