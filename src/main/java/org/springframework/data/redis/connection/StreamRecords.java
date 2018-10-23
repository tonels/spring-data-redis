package org.springframework.data.redis.connection;

import lombok.EqualsAndHashCode;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.data.redis.connection.RedisStreamCommands.ByteMapRecord;
import org.springframework.data.redis.connection.RedisStreamCommands.RecordId;
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
		return new ByteMapBackedRecord(null, RecordId.autoGenerate(), raw);
	}

	public static StringMapRecord string(Map<String, String> values) {
		return new StringMapBackedRecord(null, RecordId.autoGenerate(), values);
	}

	public static <S, K, V> MapRecord<S, K, V> mapBacked(Map<K, V> map) {
		return new MapBackedRecord<>(null, RecordId.autoGenerate(), map);
	}

	public static <S, V> ObjectRecord<S, V> objectBacked(V value) {
		return new ObjectBackedRecord<>(null, RecordId.autoGenerate(), value);
	}

	public static RecordBuilder newRecord() {
		return new RecordBuilder(null, RecordId.autoGenerate());
	}

	public static class RecordBuilder<S> {

		private RecordId id;
		private S stream;

		RecordBuilder(@Nullable S stream, RecordId recordId) {

			this.stream = stream;
			this.id = recordId;
		}

		public <STREAM_KEY> RecordBuilder<STREAM_KEY> in(STREAM_KEY stream) {
			return new RecordBuilder<>(stream, id);
		}

		public RecordBuilder<S> withId(String id) {
			return withId(RecordId.of(id));
		}

		public RecordBuilder<S> withId(RecordId id) {

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
		private RecordId recordId;
		private final Map<K, V> kvMap;

		MapBackedRecord(S stream, RecordId recordId, Map<K, V> kvMap) {

			this.stream = stream;
			this.recordId = recordId;
			this.kvMap = kvMap;
		}

		@Nullable
		@Override
		public S getStream() {
			return stream;
		}

		@Nullable
		@Override
		public RecordId getId() {
			return recordId;
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
		public MapRecord<S, K, V> withId(RecordId id) {
			return new MapBackedRecord<>(stream, id, this.kvMap);
		}

		@Override
		public <S1> MapRecord<S1, K, V> withStreamKey(S1 key) {
			return new MapBackedRecord<>(key, recordId, this.kvMap);
		}

		@Override
		public String toString() {
			return "MapBackedRecord{" + "recordId=" + recordId + ", kvMap=" + kvMap + '}';
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

			if (!ObjectUtils.nullSafeEquals(this.recordId, that.recordId)) {
				return false;
			}

			return ObjectUtils.nullSafeEquals(this.kvMap, that.kvMap);
		}

		@Override
		public int hashCode() {
			int result = stream != null ? stream.hashCode() : 0;
			result = 31 * result + recordId.hashCode();
			result = 31 * result + kvMap.hashCode();
			return result;
		}
	}

	static class ByteMapBackedRecord extends MapBackedRecord<byte[], byte[], byte[]> implements ByteMapRecord {

		ByteMapBackedRecord(byte[] stream, RecordId recordId, Map<byte[], byte[]> map) {
			super(stream, recordId, map);
		}

		@Override
		public ByteMapBackedRecord withStreamKey(byte[] key) {
			return new ByteMapBackedRecord(key, getId(), getValue());
		}

		public ByteMapBackedRecord withId(RecordId id) {
			return new ByteMapBackedRecord(getStream(), id, getValue());
		}
	}

	static class StringMapBackedRecord extends MapBackedRecord<String, String, String> implements StringMapRecord {

		StringMapBackedRecord(String stream, RecordId recordId, Map<String, String> stringStringMap) {
			super(stream, recordId, stringStringMap);
		}

		@Override
		public StringMapRecord withStreamKey(String key) {
			return new StringMapBackedRecord(key, getId(), getValue());
		}

		public StringMapBackedRecord withId(RecordId id) {
			return new StringMapBackedRecord(getStream(), id, getValue());
		}

	}

	@EqualsAndHashCode
	static class ObjectBackedRecord<S, V> implements ObjectRecord<S, V> {

		private @Nullable S stream;
		private RecordId recordId;
		private final V value;

		public ObjectBackedRecord(@Nullable S stream, RecordId recordId, V value) {

			this.stream = stream;
			this.recordId = recordId;
			this.value = value;
		}

		@Nullable
		@Override
		public S getStream() {
			return stream;
		}

		@Nullable
		@Override
		public RecordId getId() {
			return recordId;
		}

		@Override
		public V getValue() {
			return value;
		}

		@Override
		public ObjectRecord<S, V> withId(RecordId id) {
			return new ObjectBackedRecord<>(stream, id, value);
		}

		@Override
		public <S1> ObjectRecord<S1, V> withStreamKey(S1 key) {
			return new ObjectBackedRecord<>(key, recordId, value);
		}

		@Override
		public String toString() {
			return "ObjectBackedRecord{" + "recordId=" + recordId + ", value=" + value + '}';
		}
	}
}
