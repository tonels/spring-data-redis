/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.core;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisStreamCommands.Consumer;
import org.springframework.data.redis.connection.RedisStreamCommands.MapRecord;
import org.springframework.data.redis.connection.RedisStreamCommands.ObjectRecord;
import org.springframework.data.redis.connection.RedisStreamCommands.ReadOffset;
import org.springframework.data.redis.connection.RedisStreamCommands.Record;
import org.springframework.data.redis.connection.RedisStreamCommands.RecordId;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
import org.springframework.data.redis.connection.StreamRecords;
import org.springframework.data.redis.hash.HashMapper;
import org.springframework.lang.Nullable;

/**
 * Redis stream specific operations.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.2
 */
public interface StreamOperations<K, HK, HV> {

	/**
	 * Acknowledge one or more records as processed.
	 *
	 * @param key the stream key.
	 * @param group name of the consumer group.
	 * @param recordIds record id's to acknowledge.
	 * @return length of acknowledged records. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xack">Redis Documentation: XACK</a>
	 */
	@Nullable
	Long acknowledge(K key, String group, String... recordIds);

	default Long acknowledge(K key, String group, RecordId... recordIds) {
		return acknowledge(key, group, Arrays.stream(recordIds).map(RecordId::getValue).toArray(String[]::new));
	}

	default Long acknowledge(String group, Record<K, ?> record) {
		return acknowledge(record.getStream(), group, record.getId());
	}

	/**
	 * Append a record to the stream {@code key}.
	 *
	 * @param key the stream key.
	 * @param content record content as Map.
	 * @return the record Id. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xadd">Redis Documentation: XADD</a>
	 */
	@Nullable
	default RecordId add(K key, Map<HK, HV> content) {
		return add(StreamRecords.newRecord().in(key).ofMap(content));
	}

	RecordId add(MapRecord<K, HK, HV> record);

	default <V> RecordId add(Record<K, V> value) {
		return add(toMapRecord(value));
	}

	/**
	 * Removes the specified entries from the stream. Returns the number of items deleted, that may be different from the
	 * number of IDs passed in case certain IDs do not exist.
	 *
	 * @param key the stream key.
	 * @param recordIds stream record id's.
	 * @return number of removed entries. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xdel">Redis Documentation: XDEL</a>
	 */
	@Nullable
	Long delete(K key, String... recordIds);

	/**
	 * Create a consumer group.
	 *
	 * @param key
	 * @param readOffset
	 * @param group name of the consumer group.
	 * @return {@literal true} if successful. {@literal null} when used in pipeline / transaction.
	 */
	@Nullable
	String createGroup(K key, ReadOffset readOffset, String group);

	/**
	 * Delete a consumer from a consumer group.
	 *
	 * @param key the stream key.
	 * @param consumer consumer identified by group name and consumer key.
	 * @return {@literal true} if successful. {@literal null} when used in pipeline / transaction.
	 */
	@Nullable
	Boolean deleteConsumer(K key, Consumer consumer);

	/**
	 * Destroy a consumer group.
	 *
	 * @param key the stream key.
	 * @param group name of the consumer group.
	 * @return {@literal true} if successful. {@literal null} when used in pipeline / transaction.
	 */
	@Nullable
	Boolean destroyGroup(K key, String group);

	/**
	 * Get the length of a stream.
	 *
	 * @param key the stream key.
	 * @return length of the stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xlen">Redis Documentation: XLEN</a>
	 */
	@Nullable
	Long size(K key);

	/**
	 * Read records from a stream within a specific {@link Range}.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 */
	@Nullable
	default List<MapRecord<K, HK, HV>> range(K key, Range<String> range) {
		return range(key, range, Limit.unlimited());
	}

	/**
	 * Read records from a stream within a specific {@link Range} applying a {@link Limit}.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 */
	@Nullable
	List<MapRecord<K, HK, HV>> range(K key, Range<String> range, Limit limit);

	default <V> List<ObjectRecord<K, V>> range(K key, Range<String> range, Class<V> targetType) {
		return range(key, range).stream().map(it -> toObjectRecord(it, targetType)).collect(Collectors.toList());
	}

	/**
	 * Read records from one or more {@link StreamOffset}s.
	 *
	 * @param stream the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	@Nullable
	default List<MapRecord<K, HK, HV>> read(StreamOffset<K> stream) {
		return read(StreamReadOptions.empty(), new StreamOffset[] { stream });
	}

	default <V> List<ObjectRecord<K, V>> read(StreamOffset<K> stream, Class<V> targetType) {
		return read(StreamReadOptions.empty(), targetType, new StreamOffset[] { stream });
	}

	/**
	 * Read records from one or more {@link StreamOffset}s.
	 *
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	@Nullable
	default List<MapRecord<K, HK, HV>> read(StreamOffset<K>... streams) {
		return read(StreamReadOptions.empty(), streams);
	}

	/**
	 * Read records from one or more {@link StreamOffset}s.
	 *
	 * @param readOptions read arguments.
	 * @param stream the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	@Nullable
	default List<MapRecord<K, HK, HV>> read(StreamReadOptions readOptions, StreamOffset<K> stream) {
		return read(readOptions, new StreamOffset[] { stream });
	}

	/**
	 * Read records from one or more {@link StreamOffset}s.
	 *
	 * @param readOptions read arguments.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	@Nullable
	List<MapRecord<K, HK, HV>> read(StreamReadOptions readOptions, StreamOffset<K>... streams);

	default <V> List<ObjectRecord<K, V>> read(StreamReadOptions readOptions, Class<V> targetType,
			StreamOffset<K>... streams) {
		return read(readOptions, streams).stream().map(it -> toObjectRecord(it, targetType)).collect(Collectors.toList());
	}

	/**
	 * Read records from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param stream the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	@Nullable
	default List<MapRecord<K, HK, HV>> read(Consumer consumer, StreamOffset<K> stream) {
		return read(consumer, StreamReadOptions.empty(), new StreamOffset[] { stream });
	}

	/**
	 * Read records from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	@Nullable
	default List<MapRecord<K, HK, HV>> read(Consumer consumer, StreamOffset<K>... streams) {
		return read(consumer, StreamReadOptions.empty(), streams);
	}

	/**
	 * Read records from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param readOptions read arguments.
	 * @param stream the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	@Nullable
	default List<MapRecord<K, HK, HV>> read(Consumer consumer, StreamReadOptions readOptions, StreamOffset<K> stream) {
		return read(consumer, readOptions, new StreamOffset[] { stream });
	}

	/**
	 * Read records from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param readOptions read arguments.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	@Nullable
	List<MapRecord<K, HK, HV>> read(Consumer consumer, StreamReadOptions readOptions, StreamOffset<K>... streams);

	default <V> List<ObjectRecord<K, V>> read(Consumer consumer, StreamReadOptions readOptions, Class<V> targetType,
			StreamOffset<K>... streams) {
		return read(consumer, readOptions, streams).stream().map(it -> toObjectRecord(it, targetType))
				.collect(Collectors.toList());
	}

	/**
	 * Read records from a stream within a specific {@link Range} in reverse order.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	@Nullable
	default List<MapRecord<K, HK, HV>> reverseRange(K key, Range<String> range) {
		return reverseRange(key, range, Limit.unlimited());
	}

	/**
	 * Read records from a stream within a specific {@link Range} applying a {@link Limit} in reverse order.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	@Nullable
	List<MapRecord<K, HK, HV>> reverseRange(K key, Range<String> range, Limit limit);

	default <V> List<ObjectRecord<K, V>> reverseRange(K key, Range<String> range, Class<V> targetType) {
		return reverseRange(key, range).stream().map(it -> toObjectRecord(it, targetType)).collect(Collectors.toList());
	}

	/**
	 * Trims the stream to {@code count} elements.
	 *
	 * @param key the stream key.
	 * @param count length of the stream.
	 * @return number of removed entries. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xtrim">Redis Documentation: XTRIM</a>
	 */
	@Nullable
	Long trim(K key, long count);

	/**
	 * Get the {@link HashMapper} for a specific type.
	 *
	 * @param targetType must not be {@literal null}.
	 * @param <V>
	 * @return the {@link HashMapper} suitable for a given type;
	 */
	<V> HashMapper<V, HK, HV> getHashMapper(Class<V> targetType);

	/**
	 * App
	 * 
	 * @param value
	 * @param <V>
	 * @return
	 */
	default <V> MapRecord<K, HK, HV> toMapRecord(Record<K, V> value) {

		if (value instanceof ObjectRecord) {

			ObjectRecord entry = ((ObjectRecord) value);
			return entry.toMapRecord(getHashMapper(entry.getValue().getClass()));
		}

		if (value instanceof MapRecord) {
			return (MapRecord<K, HK, HV>) value;
		}

		return Record.of(((HashMapper) getHashMapper(value.getClass())).toHash(value)).withStreamKey(value.getStream());
	}

	default <V> ObjectRecord<K, V> toObjectRecord(MapRecord<K, HK, HV> entry, Class<V> targetType) {
		return entry.toObjectRecord(getHashMapper(targetType));
	}
}
