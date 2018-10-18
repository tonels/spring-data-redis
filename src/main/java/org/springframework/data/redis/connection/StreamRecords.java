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

	public static MapRecord<byte[], byte[], byte[]> rawBytes(Map<byte[], byte[]> raw) {
		return new RawEntry(null, EntryId.autoGenerate(), raw);
	}

	public static <S, K, V> MapRecord<S, K, V> mapBacked(Map<K, V> map) {
		return new MapBackedRecord<>(null, EntryId.autoGenerate(), map);
	}

	public static <S, V> ObjectRecord<S, V> objectBacked(V value) {
		return new ObjectBackedRecord<>(null, EntryId.autoGenerate(), value);
	}

	public static RecordBuilder newRecord() {
		return new RecordBuilder(null, EntryId.autoGenerate());
	}

	static class RawEntry extends MapBackedRecord<byte[], byte[], byte[]> {
		public RawEntry(byte[] stream, EntryId entryId, Map<byte[], byte[]> map) {
			super(stream, entryId, map);
		}
	}

	public static class RecordBuilder<S> {

		private EntryId id;
		private S stream;

		RecordBuilder(@Nullable S stream, EntryId entryId) {

			this.stream = stream;
			this.id = entryId;
		}

		public <STREAM_KEY> RecordBuilder<STREAM_KEY> in(STREAM_KEY stream) {
			return new RecordBuilder<>(stream, id);
		}

		public RecordBuilder<S> withId(String id) {
			return withId(EntryId.of(id));
		}

		public RecordBuilder<S> withId(EntryId id) {

			this.id = id;
			return this;
		}

		public <K, V> MapRecord<S, K, V> ofMap(Map<K, V> map) {
			return new MapBackedRecord<>(stream, id, map);
		}

		public <V> ObjectRecord<S, V> ofObject(V value) {
			return new ObjectBackedRecord<>(stream, id, value);
		}

	}

	static class MapBackedRecord<S, K, V> implements MapRecord<S, K, V> {

		private @Nullable S stream;
		private EntryId entryId;
		private final Map<K, V> kvMap;

		MapBackedRecord(S stream, EntryId entryId, Map<K, V> kvMap) {

			this.stream = stream;
			this.entryId = entryId;
			this.kvMap = kvMap;
		}

		@Nullable
		@Override
		public S getStream() {
			return stream;
		}

		@Nullable
		@Override
		public EntryId getId() {
			return entryId;
		}

		@Override
		public <K1, V1> MapBackedRecord<S, K1, V1> mapEntries(Function<Entry<K, V>, Entry<K1, V1>> mapFunction) {

			Map<K1, V1> mapped = new LinkedHashMap<>(kvMap.size(), 1);
			iterator().forEachRemaining(it -> {

				Entry<K1, V1> mappedPair = mapFunction.apply(it);
				mapped.put(mappedPair.getKey(), mappedPair.getValue());
			});

			return new MapBackedRecord<>(stream, entryId, mapped);
		}

		@Override
		public <S1, HK, HV> MapRecord<S1, HK, HV> map(Function<MapRecord<S, K, V>, MapRecord<S1, HK, HV>> mapFunction) {
			return mapFunction.apply(this);
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
		public MapRecord<S, K, V> withId(EntryId id) {
			return new MapBackedRecord<>(stream, id, this.kvMap);
		}

		@Override
		public <S1> MapRecord<S1, K, V> withStreamKey(S1 key) {
			return new MapBackedRecord<>(key, entryId, this.kvMap);
		}

		@Override
		public String toString() {
			return "MapBackedRecord{" + "entryId=" + entryId + ", kvMap=" + kvMap + '}';
		}
	}

	static class ObjectBackedRecord<S, V> implements ObjectRecord<S, V> {

		private @Nullable S stream;
		private EntryId entryId;
		private final V value;

		public ObjectBackedRecord(@Nullable S stream, EntryId entryId, V value) {

			this.stream = stream;
			this.entryId = entryId;
			this.value = value;
		}

		@Nullable
		@Override
		public S getStream() {
			return null;
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
		public ObjectRecord<S, V> withId(EntryId id) {
			return new ObjectBackedRecord<>(stream, id, value);
		}

		@Override
		public <S1> ObjectRecord<S1, V> withStreamKey(S1 key) {
			return new ObjectBackedRecord<>(key, entryId, value);
		}

		@Override
		public String toString() {
			return "ObjectBackedRecord{" + "entryId=" + entryId + ", value=" + value + '}';
		}
	}
}
