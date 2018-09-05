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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStreamCommands.Consumer;
import org.springframework.data.redis.connection.RedisStreamCommands.ReadOffset;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamMessage;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
import org.springframework.lang.Nullable;

/**
 * Default implementation of {@link ListOperations}.
 *
 * @author Mark Paluch
 */
class DefaultStreamOperations<K, V> extends AbstractOperations<K, V> implements StreamOperations<K, V> {

	DefaultStreamOperations(RedisTemplate<K, V> template) {
		super(template);
	}

	@Override
	public Long acknowledge(K key, String group, String... messageIds) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xAck(rawKey, group, messageIds), true);
	}

	@Override
	public String add(K key, Map<K, V> body) {

		byte[] rawKey = rawKey(key);
		Map<byte[], byte[]> rawBody = new LinkedHashMap<>(body.size());

		for (Map.Entry<? extends K, ? extends V> entry : body.entrySet()) {
			rawBody.put(rawKey(entry.getKey()), rawValue(entry.getValue()));
		}

		return execute(connection -> connection.xAdd(rawKey, rawBody), true);
	}

	@Override
	public Long delete(K key, String... messageIds) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xDel(rawKey, messageIds), true);
	}

	@Override
	public String createGroup(K key, ReadOffset readOffset, String group) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xGroupCreate(rawKey, readOffset, group), true);
	}

	@Override
	public Boolean deleteConsumer(K key, Consumer consumer) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xGroupDelConsumer(rawKey, consumer), true);
	}

	@Override
	public Boolean destroyGroup(K key, String group) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xGroupDestroy(rawKey, group), true);
	}

	@Override
	public Long size(K key) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xLen(rawKey), true);
	}

	@Override
	public List<StreamMessage<K, V>> range(K key, Range<String> range, Limit limit) {

		return execute(new StreamMessagesDeserializingRedisCallback() {
			@Nullable
			@Override
			List<StreamMessage<byte[], byte[]>> inRedis(RedisConnection connection) {
				return connection.xRange(rawKey(key), range, limit);
			}
		}, true);
	}

	@Override
	public List<StreamMessage<K, V>> read(StreamReadOptions readOptions, StreamOffset<K>... streams) {

		return execute(new StreamMessagesDeserializingRedisCallback() {
			@Nullable
			@Override
			List<StreamMessage<byte[], byte[]>> inRedis(RedisConnection connection) {
				return connection.xRead(readOptions, rawStreamOffsets(streams));
			}
		}, true);
	}

	@Override
	public List<StreamMessage<K, V>> read(Consumer consumer, StreamReadOptions readOptions, StreamOffset<K>... streams) {

		return execute(new StreamMessagesDeserializingRedisCallback() {
			@Nullable
			@Override
			List<StreamMessage<byte[], byte[]>> inRedis(RedisConnection connection) {
				return connection.xReadGroup(consumer, readOptions, rawStreamOffsets(streams));
			}
		}, true);
	}

	@Override
	public List<StreamMessage<K, V>> reverseRange(K key, Range<String> range, Limit limit) {

		return execute(new StreamMessagesDeserializingRedisCallback() {
			@Nullable
			@Override
			List<StreamMessage<byte[], byte[]>> inRedis(RedisConnection connection) {
				return connection.xRevRange(rawKey(key), range, limit);
			}
		}, true);
	}

	@Override
	public Long trim(K key, long count) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xTrim(rawKey, count), true);
	}

	@SuppressWarnings("unchecked")
	private Map<K, V> deserializeBody(@Nullable Map<byte[], byte[]> entries) {
		// connection in pipeline/multi mode

		if (entries == null) {
			return null;
		}

		Map<K, V> map = new LinkedHashMap<>(entries.size());

		for (Map.Entry<byte[], byte[]> entry : entries.entrySet()) {
			map.put(deserializeKey(entry.getKey()), deserializeValue(entry.getValue()));
		}

		return map;
	}

	private StreamOffset<byte[]>[] rawStreamOffsets(StreamOffset<K>[] streams) {

		return Arrays.stream(streams) //
				.map(it -> StreamOffset.create(rawKey(it.getKey()), it.getOffset())) //
				.toArray(it -> new StreamOffset[it]);
	}

	abstract class StreamMessagesDeserializingRedisCallback implements RedisCallback<List<StreamMessage<K, V>>> {

		public final List<StreamMessage<K, V>> doInRedis(RedisConnection connection) {

			List<StreamMessage<byte[], byte[]>> streamMessages = inRedis(connection);

			if (streamMessages == null) {
				return null;
			}

			List<StreamMessage<K, V>> result = new ArrayList<>(streamMessages.size());

			for (StreamMessage<byte[], byte[]> streamMessage : streamMessages) {

				result.add(new StreamMessage<>(deserializeKey(streamMessage.getStream()), streamMessage.getId(),
						deserializeBody(streamMessage.getBody())));
			}

			return result;
		}

		@Nullable
		abstract List<StreamMessage<byte[], byte[]>> inRedis(RedisConnection connection);
	}
}
