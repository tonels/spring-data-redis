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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.Map;

import org.reactivestreams.Publisher;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisStreamCommands.Consumer;
import org.springframework.data.redis.connection.RedisStreamCommands.MapRecord;
import org.springframework.data.redis.connection.RedisStreamCommands.Record;
import org.springframework.data.redis.connection.RedisStreamCommands.RecordId;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
import org.springframework.data.redis.connection.StreamRecords;

/**
 * Redis stream specific operations.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.2
 */
public interface ReactiveStreamOperations<K, HK, HV> {

	/**
	 * Acknowledge one or more messages as processed.
	 *
	 * @param key the stream key.
	 * @param group name of the consumer group.
	 * @param recordIds message Id's to acknowledge.
	 * @return length of acknowledged messages.
	 * @see <a href="http://redis.io/commands/xack">Redis Documentation: XACK</a>
	 */
	default Mono<Long> acknowledge(K key, String group, String... recordIds) {
		return acknowledge(key, group, Arrays.stream(recordIds).map(RecordId::of).toArray(RecordId[]::new));
	}

	Mono<Long> acknowledge(K key, String group, RecordId... recordIds);

	default Mono<Long> acknowledge(String group, Record<K, ?> record) {
		return acknowledge(record.getStream(), group, record.getId());
	}

	/**
	 * Append one or more message to the stream {@code key}.
	 *
	 * @param key the stream key.
	 * @param bodyPublisher message body {@link Publisher}.
	 * @return the message Ids.
	 * @see <a href="http://redis.io/commands/xadd">Redis Documentation: XADD</a>
	 */
	default Flux<RecordId> add(K key, Publisher<? extends Map<HK, HV>> bodyPublisher) {
		return Flux.from(bodyPublisher).flatMap(it -> add(key, it));
	}

	/**
	 * Append a message to the stream {@code key}.
	 *
	 * @param key the stream key.
	 * @param body message body.
	 * @return the message Id.
	 * @see <a href="http://redis.io/commands/xadd">Redis Documentation: XADD</a>
	 */
	default Mono<RecordId> add(K key, Map<HK, HV> body) {
		return add(StreamRecords.newRecord().in(key).ofMap(body));
	}

	Mono<RecordId> add(MapRecord<K, HK, HV> record);

	/**
	 * Removes the specified entries from the stream. Returns the number of items deleted, that may be different from the
	 * number of IDs passed in case certain IDs do not exist.
	 *
	 * @param key the stream key.
	 * @param recordIds stream message Id's.
	 * @return number of removed entries.
	 * @see <a href="http://redis.io/commands/xdel">Redis Documentation: XDEL</a>
	 */
	default Mono<Long> delete(K key, String... recordIds) {
		return delete(key, Arrays.stream(recordIds).map(RecordId::of).toArray(RecordId[]::new));
	}

	default Mono<Long> delete(Record<K, ?> record) {
		return delete(record.getStream(), record.getId());
	}

	Mono<Long> delete(K key, RecordId... recordIds);

	/**
	 * Get the length of a stream.
	 *
	 * @param key the stream key.
	 * @return length of the stream.
	 * @see <a href="http://redis.io/commands/xlen">Redis Documentation: XLEN</a>
	 */
	Mono<Long> size(K key);

	/**
	 * Read messages from a stream within a specific {@link Range}.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 */
	default Flux<MapRecord<K, HK, HV>> range(K key, Range<String> range) {
		return range(key, range, Limit.unlimited());
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
	Flux<MapRecord<K, HK, HV>> range(K key, Range<String> range, Limit limit);

	/**
	 * Read messages from one or more {@link StreamOffset}s.
	 *
	 * @param stream the streams to read from.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	default Flux<MapRecord<K, HK, HV>> read(StreamOffset<K> stream) {
		return read(StreamReadOptions.empty(), new StreamOffset[] { stream });
	}

	/**
	 * Read messages from one or more {@link StreamOffset}s.
	 *
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	default Flux<MapRecord<K, HK, HV>> read(StreamOffset<K>... streams) {
		return read(StreamReadOptions.empty(), streams);
	}

	/**
	 * Read messages from one or more {@link StreamOffset}s.
	 *
	 * @param readOptions read arguments.
	 * @param stream the streams to read from.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	default Flux<MapRecord<K, HK, HV>> read(StreamReadOptions readOptions, StreamOffset<K> stream) {
		return read(readOptions, new StreamOffset[] { stream });
	}

	/**
	 * Read messages from one or more {@link StreamOffset}s.
	 *
	 * @param readOptions read arguments.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	Flux<MapRecord<K, HK, HV>> read(StreamReadOptions readOptions, StreamOffset<K>... streams);

	/**
	 * Read messages from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param stream the streams to read from.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	default Flux<MapRecord<K, HK, HV>> read(Consumer consumer, StreamOffset<K> stream) {
		return read(consumer, StreamReadOptions.empty(), new StreamOffset[] { stream });
	}

	/**
	 * Read messages from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	default Flux<MapRecord<K, HK, HV>> read(Consumer consumer, StreamOffset<K>... streams) {
		return read(consumer, StreamReadOptions.empty(), streams);
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
	default Flux<MapRecord<K, HK, HV>> read(Consumer consumer, StreamReadOptions readOptions, StreamOffset<K> stream) {
		return read(consumer, readOptions, new StreamOffset[] { stream });
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
	Flux<MapRecord<K, HK, HV>> read(Consumer consumer, StreamReadOptions readOptions, StreamOffset<K>... streams);

	/**
	 * Read messages from a stream within a specific {@link Range} in reverse order.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	default Flux<MapRecord<K, HK, HV>> reverseRange(K key, Range<String> range) {
		return reverseRange(key, range, Limit.unlimited());
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
	Flux<MapRecord<K, HK, HV>> reverseRange(K key, Range<String> range, Limit limit);

	/**
	 * Trims the stream to {@code count} elements.
	 *
	 * @param key the stream key.
	 * @param count length of the stream.
	 * @return number of removed entries.
	 * @see <a href="http://redis.io/commands/xtrim">Redis Documentation: XTRIM</a>
	 */
	Mono<Long> trim(K key, long count);
}
