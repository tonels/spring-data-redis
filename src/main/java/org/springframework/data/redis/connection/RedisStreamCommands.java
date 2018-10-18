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
package org.springframework.data.redis.connection;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
import org.springframework.data.redis.hash.HashMapper;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.NumberUtils;
import org.springframework.util.StringUtils;

/**
 * Stream-specific Redis commands.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @see <a href="https://redis.io/topics/streams-intro">Redis Documentation - Streams</a>
 * @since 2.2
 */
public interface RedisStreamCommands {

	// TODO: remove when done
	static List<StreamMessage<byte[], byte[]>> mapToStreamMessage(List<MapRecord<byte[], byte[], byte[]>> rawResult) {
		List<StreamMessage<byte[], byte[]>> messages = new ArrayList<>();
		for (MapRecord<byte[], byte[], byte[]> record : rawResult) {

			messages.add(new StreamMessage(record.getStream(), record.getId().getValue(), record.getValue()));
		}

		return messages;
	}

	/**
	 * Acknowledge one or more messages as processed.
	 *
	 * @param key the stream key.
	 * @param group name of the consumer group.
	 * @param messageIds message Id's to acknowledge.
	 * @return length of acknowledged messages. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xack">Redis Documentation: XACK</a>
	 */
	@Nullable
	default Long xAck(byte[] key, String group, String... messageIds) {
		return xAck(key, group, Arrays.stream(messageIds).map(EntryId::of).toArray(EntryId[]::new));
	}

	@Nullable
	Long xAck(byte[] key, String group, EntryId... entryIds);

	/**
	 * Append a message to the stream {@code key}.
	 *
	 * @param key the stream key.
	 * @param body message body.
	 * @return the message Id. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xadd">Redis Documentation: XADD</a>
	 */
	@Nullable
	default EntryId xAdd(byte[] key, Map<byte[], byte[]> body) {
		return xAdd(StreamRecords.newRecord().in(key).ofMap(body));
	}

	/**
	 * Append the given {@link MapRecord record} to the stream at {@literal key}.
	 *
	 * @param record the {@link MapRecord record} to append.
	 * @return the {@link EntryId entry-id} after save.
	 */
	EntryId xAdd(MapRecord<byte[], byte[], byte[]> record);

	/**
	 * Removes the specified entries from the stream. Returns the number of items deleted, that may be different from the
	 * number of IDs passed in case certain IDs do not exist.
	 *
	 * @param key the stream key.
	 * @param messageIds stream message Id's.
	 * @return number of removed entries. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xdel">Redis Documentation: XDEL</a>
	 */
	@Nullable
	default Long xDel(byte[] key, String... messageIds) {
		return xDel(key, Arrays.stream(messageIds).map(EntryId::of).toArray(EntryId[]::new));
	}

	Long xDel(byte[] key, EntryId... entryIds);

	/**
	 * Create a consumer group.
	 *
	 * @param key the stream key.
	 * @param readOffset the offset to start with.
	 * @param group name of the consumer group.
	 * @return {@literal true} if successful. {@literal null} when used in pipeline / transaction.
	 */
	@Nullable
	String xGroupCreate(byte[] key, ReadOffset readOffset, String group);

	/**
	 * Delete a consumer from a consumer group.
	 *
	 * @param key the stream key.
	 * @param consumer consumer identified by group name and consumer key.
	 * @return {@literal true} if successful. {@literal null} when used in pipeline / transaction.
	 */
	@Nullable
	Boolean xGroupDelConsumer(byte[] key, Consumer consumer);

	/**
	 * Destroy a consumer group.
	 *
	 * @param key the stream key.
	 * @param group name of the consumer group.
	 * @return {@literal true} if successful. {@literal null} when used in pipeline / transaction.
	 */
	@Nullable
	Boolean xGroupDestroy(byte[] key, String group);

	/**
	 * Get the length of a stream.
	 *
	 * @param key the stream key.
	 * @return length of the stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xlen">Redis Documentation: XLEN</a>
	 */
	@Nullable
	Long xLen(byte[] key);

	/**
	 * Read messages from a stream within a specific {@link Range}.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 */
	@Nullable
	default List<MapRecord<byte[], byte[], byte[]>> xRange(byte[] key, Range<String> range) {
		return xRange(key, range, Limit.unlimited());
	}

	/**
	 * Read messages from a stream within a specific {@link Range} applying a {@link Limit}.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 */
	@Nullable
	List<MapRecord<byte[], byte[], byte[]>> xRange(byte[] key, Range<String> range, Limit limit);

	/**
	 * Read messages from one or more {@link StreamOffset}s.
	 *
	 * @param stream the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	@Nullable
	default List<MapRecord<byte[], byte[], byte[]>> xRead(StreamOffset<byte[]> stream) {
		return xRead(StreamReadOptions.empty(), new StreamOffset[] { stream });
	}

	/**
	 * Read messages from one or more {@link StreamOffset}s.
	 *
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	@Nullable
	default List<MapRecord<byte[], byte[], byte[]>> xRead(StreamOffset<byte[]>... streams) {
		return xRead(StreamReadOptions.empty(), streams);
	}

	/**
	 * Read messages from one or more {@link StreamOffset}s.
	 *
	 * @param readOptions read arguments.
	 * @param stream the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	@Nullable
	default List<MapRecord<byte[], byte[], byte[]>> xRead(StreamReadOptions readOptions, StreamOffset<byte[]> stream) {
		return xRead(readOptions, new StreamOffset[] { stream });
	}

	/**
	 * Read messages from one or more {@link StreamOffset}s.
	 *
	 * @param readOptions read arguments.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	@Nullable
	List<MapRecord<byte[], byte[], byte[]>> xRead(StreamReadOptions readOptions, StreamOffset<byte[]>... streams);

	/**
	 * Read messages from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param stream the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	@Nullable
	default List<MapRecord<byte[], byte[], byte[]>> xReadGroup(Consumer consumer, StreamOffset<byte[]> stream) {
		return xReadGroup(consumer, StreamReadOptions.empty(), new StreamOffset[] { stream });
	}

	/**
	 * Read messages from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	@Nullable
	default List<MapRecord<byte[], byte[], byte[]>> xReadGroup(Consumer consumer, StreamOffset<byte[]>... streams) {
		return xReadGroup(consumer, StreamReadOptions.empty(), streams);
	}

	/**
	 * Read messages from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param readOptions read arguments.
	 * @param stream the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	@Nullable
	default List<MapRecord<byte[], byte[], byte[]>> xReadGroup(Consumer consumer, StreamReadOptions readOptions,
			StreamOffset<byte[]> stream) {
		return xReadGroup(consumer, readOptions, new StreamOffset[] { stream });
	}

	/**
	 * Read messages from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param readOptions read arguments.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	@Nullable
	List<MapRecord<byte[], byte[], byte[]>> xReadGroup(Consumer consumer, StreamReadOptions readOptions,
			StreamOffset<byte[]>... streams);

	/**
	 * Read messages from a stream within a specific {@link Range} in reverse order.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	@Nullable
	default List<MapRecord<byte[],byte[], byte[]>> xRevRange(byte[] key, Range<String> range) {
		return xRevRange(key, range, Limit.unlimited());
	}

	/**
	 * Read messages from a stream within a specific {@link Range} applying a {@link Limit} in reverse order.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	@Nullable
	List<MapRecord<byte[], byte[], byte[]>> xRevRange(byte[] key, Range<String> range, Limit limit);

	/**
	 * Trims the stream to {@code count} elements.
	 *
	 * @param key the stream key.
	 * @param count length of the stream.
	 * @return number of removed entries. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xtrim">Redis Documentation: XTRIM</a>
	 */
	@Nullable
	Long xTrim(byte[] key, long count);

	/**
	 * A stream message and its id.
	 *
	 * @author Mark Paluch
	 */
	@EqualsAndHashCode
	@Getter
	class StreamMessage<K, V> {

		private final K stream;
		private final String id;
		private final Map<K, V> body;

		/**
		 * Create a new {@link io.lettuce.core.StreamMessage}.
		 *
		 * @param stream the stream.
		 * @param id the message id.
		 * @param body map containing the message body.
		 */
		public StreamMessage(K stream, String id, Map<K, V> body) {

			this.stream = stream;
			this.id = id;
			this.body = body;
		}

		@Override
		public String toString() {
			return String.format("StreamMessage[%s:%s]%s", stream, id, body);
		}
	}

	/**
	 * Value object representing read offset for a Stream.
	 */
	@EqualsAndHashCode
	@ToString
	@Getter
	class ReadOffset {

		private final String offset;

		private ReadOffset(String offset) {
			this.offset = offset;
		}

		/**
		 * Read from the latest offset.
		 *
		 * @return
		 */
		public static ReadOffset latest() {
			return new ReadOffset("$");
		}

		/**
		 * Read all new arriving elements with ids greater than the last one consumed by the consumer group.
		 *
		 * @return the {@link ReadOffset} object without a specific offset.
		 */
		public static ReadOffset lastConsumed() {
			return new ReadOffset(">");
		}

		/**
		 * Read all arriving elements from the stream starting at {@code offset}.
		 *
		 * @param offset the stream offset.
		 * @return the {@link StreamOffset} object without a specific offset.
		 */
		public static ReadOffset from(String offset) {

			Assert.hasText(offset, "Offset must not be empty");

			return new ReadOffset(offset);
		}
	}

	/**
	 * Value object representing a Stream Id with its offset.
	 */
	@EqualsAndHashCode
	@ToString
	@Getter
	class StreamOffset<K> {

		private final K key;
		private final ReadOffset offset;

		private StreamOffset(K key, ReadOffset offset) {
			this.key = key;
			this.offset = offset;
		}

		/**
		 * Create a {@link StreamOffset} given {@code key} and {@link ReadOffset}.
		 *
		 * @return
		 */
		public static <K> StreamOffset<K> create(K key, ReadOffset readOffset) {
			return new StreamOffset<>(key, readOffset);
		}
	}

	/**
	 * Options for reading messages from a Redis Stream.
	 */
	@EqualsAndHashCode
	@ToString
	@Getter
	class StreamReadOptions {

		private static final StreamReadOptions EMPTY = new StreamReadOptions(null, null, false);

		private final @Nullable Long block;
		private final @Nullable Long count;
		private final boolean noack;

		private StreamReadOptions(@Nullable Long block, @Nullable Long count, boolean noack) {
			this.block = block;
			this.count = count;
			this.noack = noack;
		}

		/**
		 * Creates an empty {@link StreamReadOptions} instance.
		 *
		 * @return an empty {@link StreamReadOptions} instance.
		 */
		public static StreamReadOptions empty() {
			return EMPTY;
		}

		/**
		 * Disable auto-acknowledgement when reading in the context of a consumer group.
		 *
		 * @return {@link StreamReadOptions} with {@code noack} applied.
		 */
		public StreamReadOptions noack() {
			return new StreamReadOptions(block, count, true);
		}

		/**
		 * Use a blocking read and supply the {@link Duration timeout} after which the call will terminate if no message was
		 * read.
		 *
		 * @param timeout the timeout for the blocking read, must not be {@literal null} or negative.
		 * @return {@link StreamReadOptions} with {@code block} applied.
		 */
		public StreamReadOptions block(Duration timeout) {

			Assert.notNull(timeout, "Block timeout must not be null!");
			Assert.isTrue(!timeout.isNegative(), "Block timeout must not be negative!");

			return new StreamReadOptions(timeout.toMillis(), count, noack);
		}

		/**
		 * Limit the number of messages returned per stream.
		 *
		 * @param count the maximum number of messages to read.
		 * @return {@link StreamReadOptions} with {@code count} applied.
		 */
		public StreamReadOptions count(long count) {

			Assert.isTrue(count > 0, "Count must be greater or equal to zero!");

			return new StreamReadOptions(block, count, noack);
		}
	}

	/**
	 * Value object representing a Stream consumer within a consumer group. Group name and consumer name are encoded as
	 * keys.
	 */
	@EqualsAndHashCode
	@Getter
	class Consumer {

		private final String group;
		private final String name;

		private Consumer(String group, String name) {
			this.group = group;
			this.name = name;
		}

		/**
		 * Create a new consumer.
		 *
		 * @param group name of the consumer group, must not be {@literal null} or empty.
		 * @param name name of the consumer, must not be {@literal null} or empty.
		 * @return the consumer {@link io.lettuce.core.Consumer} object.
		 */
		public static Consumer from(String group, String name) {

			Assert.hasText(group, "Group must not be null");
			Assert.hasText(name, "Name must not be null");

			return new Consumer(group, name);
		}

		@Override
		public String toString() {
			return String.format("%s:%s", group, name);
		}
	}

	/**
	 * The id of a single {@link Record} within a stream. Composed of two parts:
	 * {@literal <millisecondsTime>-<sequenceNumber>}.
	 *
	 * @author Christoph Strobl
	 * @see <a href="https://redis.io/topics/streams-intro#entry-ids">Redis Documentation - Entriy ID</a>
	 */
	@EqualsAndHashCode
	class EntryId {

		private static final String GENERATE_ID = "*";
		private static final String DELIMINATOR = "-";

		/**
		 * Auto-generation of IDs by the server is almost always what you want so we've got this instance here shortcutting
		 * computation.
		 */
		private static final EntryId AUTOGENERATED = new EntryId(GENERATE_ID) {

			@Override
			public Long getSequence() {
				return null;
			}

			@Override
			public Long getTimestamp() {
				return null;
			}

			@Override
			public boolean shouldBeAutoGenerated() {
				return true;
			}
		};

		private final String raw;

		/**
		 * Private constructor - validate input in static initializer blocks.
		 *
		 * @param raw
		 */
		private EntryId(String raw) {
			this.raw = raw;
		}

		/**
		 * Obtain an instance of {@link EntryId} using the provided String formatted as
		 * {@literal <millisecondsTime>-<sequenceNumber>}. <br />
		 * For server auto generated {@literal entry-id} on insert pass in {@literal null} or {@literal *}. Event better,
		 * just use {@link #autoGenerate()}.
		 *
		 * @param value can be {@literal null}.
		 * @return new instance of {@link EntryId} if no autogenerated one requested.
		 */
		public static EntryId of(@Nullable String value) {

			if (value == null || GENERATE_ID.equals(value)) {
				return autoGenerate();
			}

			Assert.isTrue(value.contains(DELIMINATOR),
					"Invalid id format. Please use the 'millisecondsTime-sequenceNumber' format.");
			return new EntryId(value);
		}

		/**
		 * Create a new instance of {@link EntryId} using the provided String formatted as
		 * {@literal <millisecondsTime>-<sequenceNumber>}. <br />
		 * For server auto generated {@literal entry-id} on insert use {@link #autoGenerate()}.
		 *
		 * @param millisecondsTime
		 * @param sequenceNumber
		 * @return new instance of {@link EntryId}.
		 */
		public static EntryId of(long millisecondsTime, long sequenceNumber) {
			return of(millisecondsTime + DELIMINATOR + sequenceNumber);
		}

		/**
		 * Obtain the {@link EntryId} signalling the server to auto generate an {@literal entry-id} on insert
		 * ({@code XADD}).
		 *
		 * @return {@link EntryId} instance signalling {@link #shouldBeAutoGenerated()}.
		 */
		public static EntryId autoGenerate() {
			return AUTOGENERATED;
		}

		/**
		 * Get the {@literal entry-id millisecondsTime} part or {@literal null} if it {@link #shouldBeAutoGenerated()}.
		 *
		 * @return millisecondsTime of the {@literal entry-id}. Can be {@literal null}.
		 */
		@Nullable
		public Long getTimestamp() {
			return value(0);
		}

		/**
		 * Get the {@literal entry-id sequenceNumber} part or {@literal null} if it {@link #shouldBeAutoGenerated()}.
		 *
		 * @return sequenceNumber of the {@literal entry-id}. Can be {@literal null}.
		 */
		@Nullable
		public Long getSequence() {
			return value(1);
		}

		/**
		 * @return {@literal true} if a new {@literal entry-id} shall be generated on server side when calling {@code XADD}.
		 */
		public boolean shouldBeAutoGenerated() {
			return false;
		}

		/**
		 * @return get the string representation of the {@literal entry-id} in
		 *         {@literal <millisecondsTime>-<sequenceNumber>} format or {@literal *} if it
		 *         {@link #shouldBeAutoGenerated()}. Never {@literal null}.
		 */
		public String getValue() {
			return raw;
		}

		@Override
		public String toString() {
			return raw;
		}

		private Long value(int index) {
			return NumberUtils.parseNumber(StringUtils.split(raw, DELIMINATOR)[index], Long.class);
		}
	}

	/**
	 * A single entry in the stream consisting of the {@link EntryId entry-id} and the actual entry-value (typically a
	 * collection of {@link MapRecord field/value pairs}).
	 *
	 * @param <V> the type backing the {@link Record}.
	 * @author Christoph Strobl
	 * @see <a href="https://redis.io/topics/streams-intro#streams-basics">Redis Documentation - Stream Basics</a>
	 */
	interface Record<S, V> {

		/**
		 * The id of the streak (aka key).
		 *
		 * @return can be {@literal null}.
		 */
		@Nullable
		S getStream();

		/**
		 * The id of the entry inside the stream.
		 *
		 * @return never {@literal null}.
		 */
		EntryId getId();

		/**
		 * @return the actual content. Never {@literal null}.
		 */
		V getValue();

		/**
		 * Create a new {@link MapRecord} instance backed by the given {@link Map} holding {@literal field/value} pairs.
		 *
		 * @param map the raw map.
		 * @param <K> the key type of the given {@link Map}.
		 * @param <V> the value type of the given {@link Map}.
		 * @return new instance of {@link MapRecord}.
		 */
		static <S, K, V> MapRecord<S, K, V> of(Map<K, V> map) {

			Assert.notNull(map, "Map must not be null!");
			return StreamRecords.<S, K, V> mapBacked(map);
		}

		/**
		 * Create a new {@link ObjectRecord} instance backed by the given {@literal value}. The value may be a simple type,
		 * like {@link String} or a complex one.
		 *
		 * @param value the value to persist.
		 * @param <V> the type of the backing value.
		 * @return new instance of {@link MapRecord}.
		 */
		static <S, V> ObjectRecord<S, V> of(V value) {

			Assert.notNull(value, "Value must not be null!");
			return StreamRecords.objectBacked(value);
		}

		/**
		 * Create a new instance of {@link Record} with the given {@link EntryId}.
		 *
		 * @param id must not be {@literal null}.
		 * @return new instance of {@link Record}.
		 */
		Record<S, V> withId(EntryId id);

		<S1> Record<S1, V> withStreamKey(S1 key);
	}

	/**
	 * A {@link Record} within the stream mapped to a single object. This may be a simple type, such as {@link String} or
	 * a complex one.
	 *
	 * @param <V> the type of the backing Object.
	 */
	interface ObjectRecord<S, V> extends Record<S, V> {

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.RedisStreamCommands.Record#withId(org.springframework.data.redis.connection.RedisStreamCommands.EntryId)
		 */
		@Override
		ObjectRecord<S, V> withId(EntryId id);

		<S1> ObjectRecord<S1, V> withStreamKey(S1 key);

		/**
		 * Apply the given {@link HashMapper} to the backing value to create a new {@link MapRecord}. An already assigned
		 * {@link EntryId id} is carried over to the new instance.
		 *
		 * @param mapper must not be {@literal null}.
		 * @param <HK> the key type of the resulting {@link MapRecord}.
		 * @param <HV> the value type of the resulting {@link MapRecord}.
		 * @return new instance of {@link MapRecord}.
		 */
		default <HK, HV> MapRecord<S, HK, HV> toMapRecord(HashMapper<? super V, HK, HV> mapper) {
			return Record.<S, HK, HV> of(mapper.toHash(getValue())).withId(getId());
		}
	}

	/**
	 * A {@link Record} within the stream backed by a collection of {@literal field/value} paris.
	 *
	 * @param <K> the field type of the backing collection.
	 * @param <V> the value type of the backing collection.
	 */
	interface MapRecord<S, K, V> extends Record<S, Map<K, V>>, Iterable<Map.Entry<K, V>> {

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.RedisStreamCommands.Record#withId(org.springframework.data.redis.connection.RedisStreamCommands.EntryId)
		 */
		@Override
		MapRecord<S, K, V> withId(EntryId id);

		<S1> MapRecord<S1, K, V> withStreamKey(S1 key);

		/**
		 * Apply the given {@link Function mapFunction} to each and every entry in the backing collection to create a new
		 * {@link MapRecord}.
		 *
		 * @param mapFunction must not be {@literal null}.
		 * @param <HK> the field type of the new backing collection.
		 * @param <HV> the value type of the new backing collection.
		 * @return new instance of {@link MapRecord}.
		 */
		<HK, HV> MapRecord<S, HK, HV> mapEntries(Function<Entry<K, V>, Entry<HK, HV>> mapFunction);

		<S1, HK, HV> MapRecord<S1, HK, HV> map(Function<MapRecord<S, K, V>, MapRecord<S1, HK, HV>> mapFunction);

		/**
		 * Apply the given {@link HashMapper} to the backing value to create a new {@link MapRecord}. An already assigned
		 * {@link EntryId id} is carried over to the new instance.
		 *
		 * @param mapper must not be {@literal null}.
		 * @param <OV> type of the value backing the {@link ObjectRecord}.
		 * @return new instance of {@link ObjectRecord}.
		 */
		default <OV> ObjectRecord<S, OV> toObjectRecord(HashMapper<OV, ? super K, ? super V> mapper) {
			return Record.<S, OV> of((OV) (mapper).fromHash((Map) getValue())).withId(getId());
		}
	}
}
