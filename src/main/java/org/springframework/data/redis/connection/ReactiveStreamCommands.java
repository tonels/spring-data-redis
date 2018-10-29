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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.reactivestreams.Publisher;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.ReactiveRedisConnection.CommandResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.connection.RedisStreamCommands.ByteBufferRecord;
import org.springframework.data.redis.connection.RedisStreamCommands.Consumer;
import org.springframework.data.redis.connection.RedisStreamCommands.RecordId;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamMessage;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Stream-specific Redis commands executed using reactive infrastructure.
 *
 * @author Mark Paluch
 * @since 2.2
 */
public interface ReactiveStreamCommands {

	/**
	 * {@code XACK} command parameters.
	 *
	 * @see <a href="http://redis.io/commands/xack">Redis Documentation: XACK</a>
	 */
	class AcknowledgeCommand extends KeyCommand {

		private final @Nullable String group;
		private final List<RecordId> recordIds;

		private AcknowledgeCommand(@Nullable ByteBuffer key, @Nullable String group, List<RecordId> recordIds) {

			super(key);
			this.group = group;
			this.recordIds = recordIds;
		}

		/**
		 * Creates a new {@link AcknowledgeCommand} given a {@link ByteBuffer key}.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link AcknowledgeCommand} for {@link ByteBuffer key}.
		 */
		public static AcknowledgeCommand stream(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");

			return new AcknowledgeCommand(key, null, Collections.emptyList());
		}

		/**
		 * Applies the {@literal recordIds}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param recordIds must not be {@literal null}.
		 * @return a new {@link AcknowledgeCommand} with {@literal recordIds} applied.
		 */
		public AcknowledgeCommand forRecords(String... recordIds) {

			Assert.notNull(recordIds, "MessageIds must not be null!");

			return forRecords(Arrays.stream(recordIds).map(RecordId::of).toArray(RecordId[]::new));
		}

		/**
		 * Applies the {@literal recordIds}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param recordIds must not be {@literal null}.
		 * @return a new {@link AcknowledgeCommand} with {@literal recordIds} applied.
		 */
		public AcknowledgeCommand forRecords(RecordId... recordIds) {

			Assert.notNull(recordIds, "MessageIds must not be null!");

			List<RecordId> newMessageIds = new ArrayList<>(getRecordIds().size() + recordIds.length);
			newMessageIds.addAll(getRecordIds());
			newMessageIds.addAll(Arrays.asList(recordIds));

			return new AcknowledgeCommand(getKey(), getGroup(), newMessageIds);
		}

		/**
		 * Applies the {@literal group}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param group must not be {@literal null}.
		 * @return a new {@link AcknowledgeCommand} with {@literal group} applied.
		 */
		public AcknowledgeCommand inGroup(String group) {

			Assert.notNull(group, "Group must not be null!");

			return new AcknowledgeCommand(getKey(), group, getRecordIds());
		}

		@Nullable
		public String getGroup() {
			return group;
		}

		public List<RecordId> getRecordIds() {
			return recordIds;
		}
	}

	/**
	 * Acknowledge one or more messages as processed.
	 *
	 * @param key the stream key.
	 * @param group name of the consumer group.
	 * @param recordIds message Id's to acknowledge.
	 * @return
	 * @see <a href="http://redis.io/commands/xadd">Redis Documentation: XADD</a>
	 */
	default Mono<Long> xAck(ByteBuffer key, String group, String... recordIds) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(recordIds, "MessageIds must not be null!");

		return xAck(Mono.just(AcknowledgeCommand.stream(key).inGroup(group).forRecords(recordIds))).next()
				.map(NumericResponse::getOutput);
	}

	/**
	 * Acknowledge one or more messages as processed.
	 *
	 * @param key the stream key.
	 * @param group name of the consumer group.
	 * @param recordIds message Id's to acknowledge.
	 * @return
	 * @see <a href="http://redis.io/commands/xadd">Redis Documentation: XADD</a>
	 */
	default Mono<Long> xAck(ByteBuffer key, String group, RecordId... recordIds) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(recordIds, "MessageIds must not be null!");

		return xAck(Mono.just(AcknowledgeCommand.stream(key).inGroup(group).forRecords(recordIds))).next()
				.map(NumericResponse::getOutput);
	}

	/**
	 * Acknowledge one or more messages as processed.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/xack">Redis Documentation: XACK</a>
	 */
	Flux<NumericResponse<AcknowledgeCommand, Long>> xAck(Publisher<AcknowledgeCommand> commands);

	/**
	 * {@code XADD} command parameters.
	 *
	 * @see <a href="http://redis.io/commands/xadd">Redis Documentation: XADD</a>
	 */
	class AddStreamRecord extends KeyCommand {

		private final ByteBufferRecord record;

		private AddStreamRecord(ByteBufferRecord record) {

			super(record.getStream());

			this.record = record;
		}

		/**
		 * Creates a new {@link AddStreamRecord} given {@link Map body}.
		 *
		 * @param record must not be {@literal null}.
		 * @return a new {@link AddStreamRecord}.
		 */
		public static AddStreamRecord of(ByteBufferRecord record) {

			Assert.notNull(record, "Record must not be null!");

			return new AddStreamRecord(record);
		}

		/**
		 * Creates a new {@link AddStreamRecord} given {@link Map body}.
		 *
		 * @param body must not be {@literal null}.
		 * @return a new {@link AddStreamRecord} for {@link Map}.
		 */
		public static AddStreamRecord body(Map<ByteBuffer, ByteBuffer> body) {

			Assert.notNull(body, "Body must not be null!");

			return new AddStreamRecord(StreamRecords.rawBuffer(body));
		}

		/**
		 * Applies the Geo set {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link ReactiveGeoCommands.GeoAddCommand} with {@literal key} applied.
		 */
		public AddStreamRecord to(ByteBuffer key) {
			return new AddStreamRecord(record.withStreamKey(key));
		}

		/**
		 * @return
		 */
		public Map<ByteBuffer, ByteBuffer> getBody() {
			return record.getValue();
		}

		public ByteBufferRecord getRecord() {
			return record;
		}
	}

	/**
	 * Add stream message with given {@literal body} to {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param body must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/xadd">Redis Documentation: XADD</a>
	 */
	default Mono<String> xAdd(ByteBuffer key, Map<ByteBuffer, ByteBuffer> body) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(body, "Body must not be null!");

		return xAdd(StreamRecords.newRecord().in(key).ofBuffer(body));
	}

	/**
	 * Add stream message with given {@literal body} to {@literal key}.
	 *
	 * @param record must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/xadd">Redis Documentation: XADD</a>
	 */
	default Mono<String> xAdd(ByteBufferRecord record) {

		Assert.notNull(record, "Record must not be null!");

		return xAdd(Mono.just(AddStreamRecord.of(record))).next().map(CommandResponse::getOutput);
	}

	/**
	 * Add stream message with given {@literal body} to {@literal key}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/xadd">Redis Documentation: XADD</a>
	 */
	Flux<CommandResponse<AddStreamRecord, String>> xAdd(Publisher<AddStreamRecord> commands);

	/**
	 * {@code XDEL} command parameters.
	 *
	 * @see <a href="http://redis.io/commands/xdel">Redis Documentation: XDEL</a>
	 */
	class DeleteCommand extends KeyCommand {

		private final List<RecordId> recordIds;

		private DeleteCommand(@Nullable ByteBuffer key, List<RecordId> recordIds) {

			super(key);
			this.recordIds = recordIds;
		}

		/**
		 * Creates a new {@link DeleteCommand} given a {@link ByteBuffer key}.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link DeleteCommand} for {@link ByteBuffer key}.
		 */
		public static DeleteCommand stream(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");

			return new DeleteCommand(key, Collections.emptyList());
		}

		/**
		 * Applies the {@literal recordIds}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param recordIds must not be {@literal null}.
		 * @return a new {@link DeleteCommand} with {@literal recordIds} applied.
		 */
		public DeleteCommand records(String... recordIds) {

			Assert.notNull(recordIds, "RecordIds must not be null!");

			return records(Arrays.stream(recordIds).map(RecordId::of).toArray(RecordId[]::new));
		}

		/**
		 * Applies the {@literal recordIds}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param recordIds must not be {@literal null}.
		 * @return a new {@link DeleteCommand} with {@literal recordIds} applied.
		 */
		public DeleteCommand records(RecordId... recordIds) {

			Assert.notNull(recordIds, "RecordIds must not be null!");

			List<RecordId> newMessageIds = new ArrayList<>(getRecordIds().size() + recordIds.length);
			newMessageIds.addAll(getRecordIds());
			newMessageIds.addAll(Arrays.asList(recordIds));

			return new DeleteCommand(getKey(), newMessageIds);
		}

		public List<RecordId> getRecordIds() {
			return recordIds;
		}
	}

	/**
	 * Removes the specified entries from the stream. Returns the number of items deleted, that may be different from the
	 * number of IDs passed in case certain IDs do not exist.
	 *
	 * @param key the stream key.
	 * @param recordIds stream message Id's.
	 * @return number of removed entries.
	 * @see <a href="http://redis.io/commands/xdel">Redis Documentation: XDEL</a>
	 */
	default Mono<Long> xDel(ByteBuffer key, String... recordIds) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(recordIds, "RecordIds must not be null!");

		return xDel(Mono.just(DeleteCommand.stream(key).records(recordIds))).next().map(CommandResponse::getOutput);
	}

	/**
	 * Removes the specified entries from the stream. Returns the number of items deleted, that may be different from the
	 * number of IDs passed in case certain IDs do not exist.
	 *
	 * @param key the stream key.
	 * @param recordIds stream message Id's.
	 * @return number of removed entries.
	 * @see <a href="http://redis.io/commands/xdel">Redis Documentation: XDEL</a>
	 */
	default Mono<Long> xDel(ByteBuffer key, RecordId... recordIds) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(recordIds, "RecordIds must not be null!");

		return xDel(Mono.just(DeleteCommand.stream(key).records(recordIds))).next().map(CommandResponse::getOutput);
	}


	/**
	 * Removes the specified entries from the stream. Returns the number of items deleted, that may be different from the
	 * number of IDs passed in case certain IDs do not exist.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/xdel">Redis Documentation: XDEL</a>
	 */
	Flux<CommandResponse<DeleteCommand, Long>> xDel(Publisher<DeleteCommand> commands);

	/**
	 * Get the size of the stream stored at {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return length of the stream.
	 * @see <a href="http://redis.io/commands/xlen">Redis Documentation: XLEN</a>
	 */
	default Mono<Long> xLen(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null!");

		return xLen(Mono.just(new KeyCommand(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Get the size of the stream stored at {@link KeyCommand#getKey()}
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/xlen">Redis Documentation: XLEN</a>
	 */
	Flux<NumericResponse<KeyCommand, Long>> xLen(Publisher<KeyCommand> commands);

	/**
	 * {@code XRANGE}/{@code XREVRANGE} command parameters.
	 *
	 * @see <a href="http://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 * @see <a href="http://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	class RangeCommand extends KeyCommand {

		private final Range<String> range;
		private final Limit limit;

		/**
		 * Creates a new {@link RangeCommand} given a {@code key}, {@link Range}, and {@link Limit}.
		 *
		 * @param key must not be {@literal null}.
		 * @param range must not be {@literal null}.
		 * @param limit must not be {@literal null}.
		 */
		private RangeCommand(ByteBuffer key, Range<String> range, Limit limit) {

			super(key);
			this.range = range;
			this.limit = limit;
		}

		/**
		 * Creates a new {@link RangeCommand} given a {@code key}.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link RangeCommand} for {@code key}.
		 */
		public static RangeCommand stream(ByteBuffer key) {
			return new RangeCommand(key, Range.unbounded(), Limit.unlimited());
		}

		/**
		 * Applies a {@link Range}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param range must not be {@literal null}.
		 * @return a new {@link RangeCommand} with {@link Range} applied.
		 */
		public RangeCommand within(Range<String> range) {

			Assert.notNull(range, "Range must not be null!");

			return new RangeCommand(getKey(), range, getLimit());
		}

		/**
		 * Applies a {@code Limit}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param count
		 * @return a new {@link RangeCommand} with {@code limit} applied.
		 */
		public RangeCommand limit(int count) {
			return new RangeCommand(getKey(), range, Limit.unlimited().count(count));
		}

		/**
		 * Applies a {@code Limit}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param limit must not be {@literal null}.
		 * @return a new {@link RangeCommand} with {@code limit} applied.
		 */
		public RangeCommand limit(Limit limit) {

			Assert.notNull(limit, "Limit must not be null!");

			return new RangeCommand(getKey(), range, limit);
		}

		/**
		 * @return the {@link Range}.
		 */
		public Range<String> getRange() {
			return range;
		}

		/**
		 * @return the {@link Limit}.
		 */
		public Limit getLimit() {
			return limit;
		}
	}

	/**
	 * Read messages from a stream within a specific {@link Range}.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 */
	default Flux<ByteBufferRecord> xRange(ByteBuffer key, Range<String> range) {
		return xRange(key, range, Limit.unlimited());
	}

	/**
	 * Read messages from a stream within a specific {@link Range} applying a {@link Limit}.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 */
	default Flux<ByteBufferRecord> xRange(ByteBuffer key, Range<String> range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");
		Assert.notNull(limit, "Limit must not be null!");

		return xRange(Mono.just(RangeCommand.stream(key).within(range).limit(limit))).next()
				.flatMapMany(CommandResponse::getOutput);
	}

	/**
	 * Read messages from a stream within a specific {@link Range} applying a {@link Limit}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 */
	Flux<CommandResponse<RangeCommand, Flux<ByteBufferRecord>>> xRange(
			Publisher<RangeCommand> commands);

	/**
	 * {@code XRANGE}/{@code XREVRANGE} command parameters.
	 *
	 * @see <a href="http://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 * @see <a href="http://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	class ReadCommand {

		private final List<StreamOffset<ByteBuffer>> streamOffsets;
		private final @Nullable StreamReadOptions readOptions;
		private final @Nullable Consumer consumer;

		/**
		 * @param streamOffsets must not be {@literal null}.
		 * @param readOptions
		 * @param consumer
		 */
		public ReadCommand(List<StreamOffset<ByteBuffer>> streamOffsets, @Nullable StreamReadOptions readOptions,
				@Nullable Consumer consumer) {

			this.readOptions = readOptions;
			this.consumer = consumer;
			this.streamOffsets = streamOffsets;
		}

		/**
		 * Creates a new {@link ReadCommand} given a {@link StreamOffset}.
		 *
		 * @param streamOffset must not be {@literal null}.
		 * @return a new {@link ReadCommand} for {@link StreamOffset}.
		 */
		public static ReadCommand from(StreamOffset<ByteBuffer> streamOffset) {

			Assert.notNull(streamOffset, "StreamOffset must not be null!");

			return new ReadCommand(Collections.singletonList(streamOffset), StreamReadOptions.empty(), null);
		}

		/**
		 * Creates a new {@link ReadCommand} given a {@link StreamOffset}s.
		 *
		 * @param streamOffsets must not be {@literal null}.
		 * @return a new {@link ReadCommand} for {@link StreamOffset}s.
		 */
		public static ReadCommand from(StreamOffset<ByteBuffer>... streamOffsets) {

			Assert.notNull(streamOffsets, "StreamOffsets must not be null!");

			return new ReadCommand(Arrays.asList(streamOffsets), StreamReadOptions.empty(), null);
		}

		/**
		 * Applies a {@link Consumer}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param consumer must not be {@literal null}.
		 * @return a new {@link ReadCommand} with {@link Consumer} applied.
		 */
		public ReadCommand as(Consumer consumer) {

			Assert.notNull(consumer, "Consumer must not be null!");

			return new ReadCommand(getStreamOffsets(), getReadOptions(), consumer);
		}

		/**
		 * Applies the given {@link StreamReadOptions}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param options must not be {@literal null}.
		 * @return a new {@link ReadCommand} with {@link Consumer} applied.
		 */
		public ReadCommand withOptions(StreamReadOptions options) {

			Assert.notNull(options, "StreamReadOptions must not be null!");

			return new ReadCommand(getStreamOffsets(), options, getConsumer());
		}

		public List<StreamOffset<ByteBuffer>> getStreamOffsets() {
			return streamOffsets;
		}

		@Nullable
		public StreamReadOptions getReadOptions() {
			return readOptions;
		}

		@Nullable
		public Consumer getConsumer() {
			return consumer;
		}
	}

	/**
	 * Read messages from one or more {@link StreamOffset}s.
	 *
	 * @param stream the streams to read from.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	default Flux<StreamMessage<ByteBuffer, ByteBuffer>> xRead(StreamOffset<ByteBuffer> stream) {
		return xRead(StreamReadOptions.empty(), new StreamOffset[] { stream });
	}

	/**
	 * Read messages from one or more {@link StreamOffset}s.
	 *
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	default Flux<StreamMessage<ByteBuffer, ByteBuffer>> xRead(StreamOffset<ByteBuffer>... streams) {
		return xRead(StreamReadOptions.empty(), streams);
	}

	/**
	 * Read messages from one or more {@link StreamOffset}s.
	 *
	 * @param readOptions read arguments.
	 * @param stream the streams to read from.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	default Flux<StreamMessage<ByteBuffer, ByteBuffer>> xRead(StreamReadOptions readOptions,
			StreamOffset<ByteBuffer> stream) {

		Assert.notNull(readOptions, "StreamReadOptions must not be null!");
		Assert.notNull(stream, "StreamOffset must not be null!");

		return xRead(readOptions, new StreamOffset[] { stream });
	}

	/**
	 * Read messages from one or more {@link StreamOffset}s.
	 *
	 * @param readOptions read arguments.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	default Flux<StreamMessage<ByteBuffer, ByteBuffer>> xRead(StreamReadOptions readOptions,
			StreamOffset<ByteBuffer>... streams) {

		Assert.notNull(readOptions, "StreamReadOptions must not be null!");
		Assert.notNull(streams, "StreamOffsets must not be null!");

		return read(Mono.just(ReadCommand.from(streams).withOptions(readOptions))).next()
				.flatMapMany(CommandResponse::getOutput);
	}

	/**
	 * Read messages from one or more {@link StreamOffset}s.
	 *
	 * @param commands must not be {@literal null}.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 * @see <a href="http://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	default Flux<CommandResponse<ReadCommand, Flux<StreamMessage<ByteBuffer, ByteBuffer>>>> read(Publisher<ReadCommand> commands) {

		return readF(commands).map(it -> new CommandResponse<>(it.getInput(), it.getOutput().map(bar -> new StreamMessage<>(bar.getStream(), bar.getId().getValue(), bar.getValue()))));
	}

	/**
	 * Read messages from one or more {@link StreamOffset}s.
	 *
	 * @param commands must not be {@literal null}.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 * @see <a href="http://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	Flux<CommandResponse<ReadCommand, Flux<ByteBufferRecord>>> readF(Publisher<ReadCommand> commands);


	/**
	 * Read messages from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param stream the streams to read from.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	default Flux<StreamMessage<ByteBuffer, ByteBuffer>> xReadGroup(Consumer consumer, StreamOffset<ByteBuffer> stream) {
		return xReadGroup(consumer, StreamReadOptions.empty(), new StreamOffset[] { stream });
	}

	/**
	 * Read messages from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	default Flux<StreamMessage<ByteBuffer, ByteBuffer>> xReadGroup(Consumer consumer,
			StreamOffset<ByteBuffer>... streams) {
		return xReadGroup(consumer, StreamReadOptions.empty(), streams);
	}

	/**
	 * Read messages from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param readOptions read arguments.
	 * @param stream the streams to read from.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	default Flux<StreamMessage<ByteBuffer, ByteBuffer>> xReadGroup(Consumer consumer, StreamReadOptions readOptions,
			StreamOffset<ByteBuffer> stream) {
		return xReadGroup(consumer, readOptions, new StreamOffset[] { stream });
	}

	/**
	 * Read messages from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param readOptions read arguments.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	default Flux<StreamMessage<ByteBuffer, ByteBuffer>> xReadGroup(Consumer consumer, StreamReadOptions readOptions,
			StreamOffset<ByteBuffer>... streams) {

		Assert.notNull(consumer, "Consumer must not be null!");
		Assert.notNull(streams, "StreamOffsets must not be null!");
		Assert.notNull(streams, "StreamOffsets must not be null!");

		return read(Mono.just(ReadCommand.from(streams).withOptions(readOptions).as(consumer))).next()
				.flatMapMany(CommandResponse::getOutput);
	}

	/**
	 * Read messages from a stream within a specific {@link Range} in reverse order.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	default Flux<StreamMessage<ByteBuffer, ByteBuffer>> xRevRange(ByteBuffer key, Range<String> range) {
		return xRevRange(key, range, Limit.unlimited());
	}

	/**
	 * Read messages from a stream within a specific {@link Range} applying a {@link Limit} in reverse order.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	default Flux<StreamMessage<ByteBuffer, ByteBuffer>> xRevRange(ByteBuffer key, Range<String> range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");
		Assert.notNull(limit, "Limit must not be null!");

		return xRevRange(Mono.just(RangeCommand.stream(key).within(range).limit(limit))).next()
				.flatMapMany(CommandResponse::getOutput);
	}

	/**
	 * Read messages from a stream within a specific {@link Range} applying a {@link Limit} in reverse order.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	Flux<CommandResponse<RangeCommand, Flux<StreamMessage<ByteBuffer, ByteBuffer>>>> xRevRange(
			Publisher<RangeCommand> commands);

	/**
	 * {@code XTRIM} command parameters.
	 *
	 * @see <a href="http://redis.io/commands/xtrim">Redis Documentation: XTRIM</a>
	 */
	class TrimCommand extends KeyCommand {

		private @Nullable Long count;

		private TrimCommand(ByteBuffer key, @Nullable Long count) {

			super(key);
			this.count = count;
		}

		/**
		 * Creates a new {@link TrimCommand} given a {@link ByteBuffer key}.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link TrimCommand} for {@link ByteBuffer key}.
		 */
		public static TrimCommand stream(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");

			return new TrimCommand(key, null);
		}

		/**
		 * Applies the numeric {@literal count}. Constructs a new command instance with all previously configured
		 * properties.
		 *
		 * @param count
		 * @return a new {@link TrimCommand} with {@literal count} applied.
		 */
		public TrimCommand to(long count) {
			return new TrimCommand(getKey(), count);
		}

		/**
		 * @return can be {@literal null}.
		 */
		@Nullable
		public Long getCount() {
			return count;
		}
	}

	/**
	 * Trims the stream to {@code count} elements.
	 *
	 * @param key the stream key.
	 * @param count length of the stream.
	 * @return number of removed entries.
	 * @see <a href="http://redis.io/commands/xtrim">Redis Documentation: XTRIM</a>
	 */
	default Mono<Long> xTrim(ByteBuffer key, long count) {

		Assert.notNull(key, "Key must not be null!");

		return xTrim(Mono.just(TrimCommand.stream(key).to(count))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Trims the stream to {@code count} elements.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/xtrim">Redis Documentation: XTRIM</a>
	 */
	Flux<NumericResponse<KeyCommand, Long>> xTrim(Publisher<TrimCommand> commands);
}
