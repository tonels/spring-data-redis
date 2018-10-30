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

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.ReactiveStreamCommands;
import org.springframework.data.redis.connection.RedisStreamCommands.ByteBufferRecord;
import org.springframework.data.redis.connection.RedisStreamCommands.Consumer;
import org.springframework.data.redis.connection.RedisStreamCommands.MapRecord;
import org.springframework.data.redis.connection.RedisStreamCommands.RecordId;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link ReactiveStreamOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.2
 */
@RequiredArgsConstructor
class DefaultReactiveStreamOperations<K, HK, HV> implements ReactiveStreamOperations<K, HK, HV> {

	private final @NonNull ReactiveRedisTemplate<?, ?> template;

	private final @NonNull RedisSerializationContext<K, ?> serializationContext;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveStreamOperations#acknowledge(java.lang.Object, java.lang.String, java.lang.String[])
	 */
	@Override
	public Mono<Long> acknowledge(K key, String group, RecordId... recordIds) {

		Assert.notNull(key, "Key must not be null!");
		Assert.hasText(group, "Group must not be null or empty!");
		Assert.notNull(recordIds, "MessageIds must not be null!");
		Assert.notEmpty(recordIds, "MessageIds must not be empty!");

		return createMono(connection -> connection.xAck(rawKey(key), group, recordIds));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveStreamOperations#add(java.lang.Object, java.util.Map)
	 */
	@Override
	public Mono<RecordId> add(MapRecord<K, HK, HV> record) {

		Assert.notNull(record.getStream(), "Key must not be null!");
		Assert.notEmpty(record.getValue(), "Body must not be null!");

		return createMono(connection -> connection.xAdd(serializeRecord(record)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveStreamOperations#delete(java.lang.Object, java.lang.String[])
	 */
	@Override
	public Mono<Long> delete(K key, RecordId... recordIds) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(recordIds, "MessageIds must not be null!");

		return createMono(connection -> connection.xDel(rawKey(key), recordIds));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveStreamOperations#size(java.lang.Object)
	 */
	@Override
	public Mono<Long> size(K key) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.xLen(rawKey(key)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveStreamOperations#range(java.lang.Object, org.springframework.data.domain.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Flux<MapRecord<K, HK, HV>> range(K key, Range<String> range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");
		Assert.notNull(limit, "Limit must not be null!");

		return createFlux(connection -> connection.xRange(rawKey(key), range, limit).map(this::deserializeRecord));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveStreamOperations#read(org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions, org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset[])
	 */
	@Override
	public Flux<MapRecord<K, HK, HV>> read(StreamReadOptions readOptions, StreamOffset<K>... streams) {

		Assert.notNull(readOptions, "StreamReadOptions must not be null!");
		Assert.notNull(streams, "Streams must not be null!");

		return createFlux(connection -> {

			StreamOffset<ByteBuffer>[] streamOffsets = rawStreamOffsets(streams);

			return connection.xRead(readOptions, streamOffsets).map(this::deserializeRecord);
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveStreamOperations#read(org.springframework.data.redis.connection.RedisStreamCommands.Consumer, org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions, org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset[])
	 */
	@Override
	public Flux<MapRecord<K, HK, HV>> read(Consumer consumer, StreamReadOptions readOptions, StreamOffset<K>... streams) {

		Assert.notNull(consumer, "Consumer must not be null!");
		Assert.notNull(readOptions, "StreamReadOptions must not be null!");
		Assert.notNull(streams, "Streams must not be null!");

		return createFlux(connection -> {

			StreamOffset<ByteBuffer>[] streamOffsets = rawStreamOffsets(streams);

			return connection.xReadGroup(consumer, readOptions, streamOffsets).map(this::deserializeRecord);
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveStreamOperations#reverseRange(java.lang.Object, org.springframework.data.domain.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Flux<MapRecord<K, HK, HV>> reverseRange(K key, Range<String> range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");
		Assert.notNull(limit, "Limit must not be null!");

		return createFlux(connection -> connection.xRevRange(rawKey(key), range, limit).map(this::deserializeRecord));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveStreamOperations#trim(java.lang.Object, long)
	 */
	@Override
	public Mono<Long> trim(K key, long count) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.xTrim(rawKey(key), count));
	}

	@SuppressWarnings("unchecked")
	private StreamOffset<ByteBuffer>[] rawStreamOffsets(StreamOffset<K>[] streams) {

		return Arrays.stream(streams).map(it -> StreamOffset.create(rawKey(it.getKey()), it.getOffset()))
				.toArray(StreamOffset[]::new);
	}

	private <T> Mono<T> createMono(Function<ReactiveStreamCommands, Publisher<T>> function) {

		Assert.notNull(function, "Function must not be null!");

		return template.createMono(connection -> function.apply(connection.streamCommands()));
	}

	private <T> Flux<T> createFlux(Function<ReactiveStreamCommands, Publisher<T>> function) {

		Assert.notNull(function, "Function must not be null!");

		return template.createFlux(connection -> function.apply(connection.streamCommands()));
	}

	private ByteBuffer rawKey(K key) {
		return serializationContext.getKeySerializationPair().write(key);
	}

	private ByteBuffer rawHashKey(HK key) {
		return serializationContext.getHashKeySerializationPair().write(key);
	}

	private ByteBuffer rawValue(HV value) {
		return serializationContext.getHashValueSerializationPair().write(value);
	}

	private HK readHashKey(ByteBuffer buffer) {
		return (HK) serializationContext.getHashKeySerializationPair().getReader().read(buffer);
	}

	private K readKey(ByteBuffer buffer) {
		return serializationContext.getKeySerializationPair().read(buffer);
	}

	private HV deserializeHashValue(ByteBuffer buffer) {
		return (HV) serializationContext.getHashValueSerializationPair().read(buffer);
	}

	private MapRecord<K, HK, HV> deserializeRecord(ByteBufferRecord record) {
		return record.map(it -> it.mapEntries(this::deserializeRecordFields).withStreamKey(readKey(record.getStream())));
	}

	private Entry<HK, HV> deserializeRecordFields(Entry<ByteBuffer, ByteBuffer> it) {
		return Collections.singletonMap(readHashKey(it.getKey()), deserializeHashValue(it.getValue())).entrySet().iterator()
				.next();
	}

	private ByteBufferRecord serializeRecord(MapRecord<K, HK, HV> record) {
		return ByteBufferRecord
				.of(record.map(it -> it.mapEntries(this::serializeRecordFields).withStreamKey(rawKey(record.getStream()))));
	}

	private Entry<ByteBuffer, ByteBuffer> serializeRecordFields(Entry<HK, HV> it) {
		return Collections.singletonMap(rawHashKey(it.getKey()), rawValue(it.getValue())).entrySet().iterator().next();
	}
}
