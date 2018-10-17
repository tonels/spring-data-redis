package org.springframework.data.redis.connection;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import org.springframework.data.redis.connection.RedisStreamCommands.EntryId;
import org.springframework.data.redis.connection.RedisStreamCommands.MapRecord;
import org.springframework.data.redis.connection.RedisStreamCommands.ObjectRecord;
import org.springframework.lang.Nullable;

/**
 * {@link StreamRecords} provides utilities to create specific
 * {@link org.springframework.data.redis.connection.RedisStreamCommands.Record} instances.
 *
 * @author Christoph Strobl
 * @since 2.2
 */
public class StreamRecords {

	public static MapRecord<byte[], byte[]> rawBytes(Map<byte[], byte[]> raw) {
		return new RawEntry(EntryId.autoGenerate(), raw);
	}

	public static <K, V> MapRecord<K, V> mapBacked(Map<K, V> map) {
		return new MapBackedRecord<>(EntryId.autoGenerate(), map);
	}

	public static <V> ObjectRecord<V> objectBacked(V value) {
		return new ObjectBackedRecord<>(EntryId.autoGenerate(), value);
	}

	static class RawEntry extends MapBackedRecord<byte[], byte[]> {

		public RawEntry(EntryId entryId, Map<byte[], byte[]> map) {
			super(entryId, map);
		}
	}

	static class MapBackedRecord<K, V> implements MapRecord<K, V> {

		private EntryId entryId;
		private final Map<K, V> kvMap;

		MapBackedRecord(EntryId entryId, Map<K, V> kvMap) {

			this.entryId = entryId;
			this.kvMap = kvMap;
		}

		@Nullable
		@Override
		public EntryId getId() {
			return entryId;
		}

		@Override
		public <K1, V1> MapBackedRecord<K1, V1> map(Function<Entry<K, V>, Entry<K1, V1>> mapFunction) {

			Map<K1, V1> mapped = new LinkedHashMap<>(kvMap.size(), 1);
			iterator().forEachRemaining(it -> {

				Entry<K1, V1> mappedPair = mapFunction.apply(it);
				mapped.put(mappedPair.getKey(), mappedPair.getValue());
			});

			return new MapBackedRecord<>(entryId, mapped);
		}

		@Override
		public Iterator<Entry<K, V>> iterator() {
			return kvMap.entrySet().iterator();
		}

		@Override
		public Map<K, V> getValue() {
			return kvMap;
		}

		@Override
		public MapRecord<K, V> withId(EntryId id) {
			return new MapBackedRecord<>(id, this.kvMap);
		}

		@Override
		public String toString() {
			return "MapBackedRecord{" + "entryId=" + entryId + ", kvMap=" + kvMap + '}';
		}
	}

	static class ObjectBackedRecord<V> implements ObjectRecord<V> {

		private EntryId entryId;
		private final V value;

		public ObjectBackedRecord(EntryId entryId, V value) {

			this.entryId = entryId;
			this.value = value;
		}

		@Nullable
		@Override
		public EntryId getId() {
			return entryId;
		}

		@Override
		public V getValue() {
			return value;
		}

		@Override
		public ObjectRecord<V> withId(EntryId id) {
			return new ObjectBackedRecord<>(id, value);
		}

		@Override
		public String toString() {
			return "ObjectBackedRecord{" + "entryId=" + entryId + ", value=" + value + '}';
		}
	}
}
