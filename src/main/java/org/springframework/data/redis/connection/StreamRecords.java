package org.springframework.data.redis.connection;

import lombok.EqualsAndHashCode;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.data.redis.connection.RedisStreamCommands.ByteMapRecord;
import org.springframework.data.redis.connection.RedisStreamCommands.EntryId;
import org.springframework.data.redis.connection.RedisStreamCommands.MapRecord;
import org.springframework.data.redis.connection.RedisStreamCommands.ObjectRecord;
import org.springframework.data.redis.connection.RedisStreamCommands.StringMapRecord;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;

/**
 * {@link StreamRecords} provides utilities to create specific
 * {@link org.springframework.data.redis.connection.RedisStreamCommands.Record} instances.
 *
 * @author Christoph Strobl
 * @since 2.2
 */
public class StreamRecords {

	public static ByteMapRecord rawBytes(Map<byte[], byte[]> raw) {
		return new ByteMapBackedRecord(null, EntryId.autoGenerate(), raw);
	}

	public static StringMapRecord string(Map<String, String> values) {
		return new StringMapBackedRecord(null, EntryId.autoGenerate(), values);
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

		public StringMapRecord ofStrings(Map<String, String> map) {
			return new StringMapBackedRecord(ObjectUtils.nullSafeToString(stream), id, map);
		}

		public <V> ObjectRecord<S, V> ofObject(V value) {
			return new ObjectBackedRecord<>(stream, id, value);
		}

		public ByteMapRecord ofBytes(Map<byte[], byte[]> value) {

			// todo auto conversion of known values
			return new ByteMapBackedRecord((byte[]) stream, id, value);
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

		@Override
		public boolean equals(Object o) {

			if(o == null) {
				return false;
			}

			if (this == o) {
				return true;
			}

			if (!ClassUtils.isAssignable(MapBackedRecord.class, o.getClass())) {
				return false;
			}

			MapBackedRecord<?, ?, ?> that = (MapBackedRecord<?, ?, ?>) o;

			if (!ObjectUtils.nullSafeEquals(this.stream, that.stream)) {
				return false;
			}

			if (!ObjectUtils.nullSafeEquals(this.entryId, that.entryId)) {
				return false;
			}

			return ObjectUtils.nullSafeEquals(this.kvMap, that.kvMap);
		}

		@Override
		public int hashCode() {
			int result = stream != null ? stream.hashCode() : 0;
			result = 31 * result + entryId.hashCode();
			result = 31 * result + kvMap.hashCode();
			return result;
		}
	}

	static class ByteMapBackedRecord extends MapBackedRecord<byte[], byte[], byte[]> implements ByteMapRecord {

		ByteMapBackedRecord(byte[] stream, EntryId entryId, Map<byte[], byte[]> map) {
			super(stream, entryId, map);
		}

		@Override
		public ByteMapBackedRecord withStreamKey(byte[] key) {
			return new ByteMapBackedRecord(key, getId(), getValue());
		}

		public ByteMapBackedRecord withId(EntryId id) {
			return new ByteMapBackedRecord(getStream(), id, getValue());
		}
	}

	static class StringMapBackedRecord extends MapBackedRecord<String, String, String> implements StringMapRecord {

		StringMapBackedRecord(String stream, EntryId entryId, Map<String, String> stringStringMap) {
			super(stream, entryId, stringStringMap);
		}

		@Override
		public StringMapRecord withStreamKey(String key) {
			return new StringMapBackedRecord(key, getId(), getValue());
		}

		public StringMapBackedRecord withId(EntryId id) {
			return new StringMapBackedRecord(getStream(), id, getValue());
		}

	}

	@EqualsAndHashCode
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
			return stream;
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
