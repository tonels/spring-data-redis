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
import org.springframework.data.redis.connection.RedisStreamCommands;
import org.springframework.data.redis.connection.RedisStreamCommands.ByteMapRecord;
import org.springframework.data.redis.connection.RedisStreamCommands.Consumer;
import org.springframework.data.redis.connection.RedisStreamCommands.EntryId;
import org.springframework.data.redis.connection.RedisStreamCommands.MapRecord;
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
 * @since 2.2
 */
class DefaultStreamOperations<K, HK, HV> extends AbstractOperations<K, Object> implements StreamOperations<K, HK, HV> {

	DefaultStreamOperations(RedisTemplate<K, ?> template) {
		super((RedisTemplate<K, Object>) template);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.StreamOperations#acknowledge(java.lang.Object, java.lang.String, java.lang.String[])
	 */
	@Override
	public Long acknowledge(K key, String group, String... messageIds) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xAck(rawKey, group, messageIds), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.StreamOperations#add(java.lang.Object, java.util.Map)
	 */
	@Override
	public EntryId add(MapRecord<K, HK, HV> record) {

		ByteMapRecord binaryRecord = record.serialize(keySerializer(), hashKeySerializer(), hashValueSerializer());

		return execute(connection -> connection.xAdd(binaryRecord), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.StreamOperations#delete(java.lang.Object, java.lang.String[])
	 */
	@Override
	public Long delete(K key, String... messageIds) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xDel(rawKey, messageIds), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.StreamOperations#createGroup(java.lang.Object, org.springframework.data.redis.connection.RedisStreamCommands.ReadOffset, java.lang.String)
	 */
	@Override
	public String createGroup(K key, ReadOffset readOffset, String group) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xGroupCreate(rawKey, readOffset, group), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.StreamOperations#deleteConsumer(java.lang.Object, org.springframework.data.redis.connection.RedisStreamCommands.Consumer)
	 */
	@Override
	public Boolean deleteConsumer(K key, Consumer consumer) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xGroupDelConsumer(rawKey, consumer), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.StreamOperations#destroyGroup(java.lang.Object, java.lang.String)
	 */
	@Override
	public Boolean destroyGroup(K key, String group) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xGroupDestroy(rawKey, group), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.StreamOperations#size(java.lang.Object)
	 */
	@Override
	public Long size(K key) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xLen(rawKey), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.StreamOperations#range(java.lang.Object, org.springframework.data.domain.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public List<MapRecord<K, HK, HV>> range(K key, Range<String> range, Limit limit) {

		return execute(new RecordDeserializingRedisCallback<K, HK, HV>() {

			@Nullable
			@Override
			List<ByteMapRecord> inRedis(RedisConnection connection) {
				return connection.xRange(rawKey(key), range, limit);
			}
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.StreamOperations#read(org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions, org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset[])
	 */
	@Override
	public List<StreamMessage<HK, HV>> read(StreamReadOptions readOptions, StreamOffset<K>... streams) {

		return execute(new StreamMessagesDeserializingRedisCallback<K, HK, HV>() {
			@Nullable
			@Override
			List<StreamMessage<byte[], byte[]>> inRedis(RedisConnection connection) {

				List<ByteMapRecord> x = connection.xRead(readOptions, rawStreamOffsets(streams));
				List<StreamMessage<byte[], byte[]>> result = new ArrayList<>();
				for (ByteMapRecord record : x) {
					result.add(new StreamMessage<>(record.getStream(), record.getId().getValue(), record.getValue()));
				}

				return result;
			}
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.StreamOperations#read(org.springframework.data.redis.connection.RedisStreamCommands.Consumer, org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions, org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset[])
	 */
	@Override
	public List<StreamMessage<HK, HV>> read(Consumer consumer, StreamReadOptions readOptions,
			StreamOffset<K>... streams) {

		return execute(new StreamMessagesDeserializingRedisCallback<K, HK, HV>() {
			@Nullable
			@Override
			List<StreamMessage<byte[], byte[]>> inRedis(RedisConnection connection) {

				List<ByteMapRecord> x = connection.xReadGroup(consumer, readOptions, rawStreamOffsets(streams));
				return RedisStreamCommands.mapToStreamMessage(x);
			}
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.StreamOperations#reverseRange(java.lang.Object, org.springframework.data.domain.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public List<MapRecord<K, HK, HV>> reverseRange(K key, Range<String> range, Limit limit) {

		return execute(new RecordDeserializingRedisCallback<K, HK, HV>() {

			@Nullable
			@Override
			List<ByteMapRecord> inRedis(RedisConnection connection) {
				return connection.xRevRange(rawKey(key), range, limit);
			}
		}, true);

//		return execute(new StreamMessagesDeserializingRedisCallback<K, HK, HV>() {
//			@Nullable
//			@Override
//			List<StreamMessage<byte[], byte[]>> inRedis(RedisConnection connection) {
//				List<ByteMapRecord> x = connection.xRevRange(rawKey(key), range, limit);
//				return RedisStreamCommands.mapToStreamMessage(x);
//			}
//		}, true);
	}

	@Override
	public Long trim(K key, long count) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xTrim(rawKey, count), true);
	}

	@SuppressWarnings("unchecked")
	private Map<HK, HV> deserializeBody(@Nullable Map<byte[], byte[]> entries) {
		// connection in pipeline/multi mode

		if (entries == null) {
			return null;
		}

		Map<HK, HV> map = new LinkedHashMap<>(entries.size());

		for (Map.Entry<byte[], byte[]> entry : entries.entrySet()) {
			map.put(deserializeHashKey(entry.getKey()), deserializeHashValue(entry.getValue()));
		}

		return map;
	}

	@SuppressWarnings("unchecked")
	private StreamOffset<byte[]>[] rawStreamOffsets(StreamOffset<K>[] streams) {

		return Arrays.stream(streams) //
				.map(it -> StreamOffset.create(rawKey(it.getKey()), it.getOffset())) //
				.toArray(it -> new StreamOffset[it]);
	}

	abstract class StreamMessagesDeserializingRedisCallback<K, HK, HV>
			implements RedisCallback<List<StreamMessage<HK, HV>>> {

		public final List<StreamMessage<HK, HV>> doInRedis(RedisConnection connection) {

			List<StreamMessage<byte[], byte[]>> streamMessages = inRedis(connection);

			if (streamMessages == null) {
				return null;
			}

			List<StreamMessage<HK, HV>> result = new ArrayList<>(streamMessages.size());

			for (StreamMessage<byte[], byte[]> streamMessage : streamMessages) {

				result.add(new StreamMessage((K) deserializeKey(streamMessage.getStream()), streamMessage.getId(),
						(Map) deserializeBody(streamMessage.getBody())));
			}

			return result;
		}

		@Nullable
		abstract List<StreamMessage<byte[], byte[]>> inRedis(RedisConnection connection);
	}

	abstract class RecordDeserializingRedisCallback<K, HK, HV> implements RedisCallback<List<MapRecord<K, HK, HV>>> {

		public final List<MapRecord<K, HK, HV>> doInRedis(RedisConnection connection) {

			List<ByteMapRecord> x = inRedis(connection);

			List<MapRecord<K, HK, HV>> result = new ArrayList<>();
			for (ByteMapRecord record : x) {
				result.add(record.deserialize(keySerializer(), hashKeySerializer(), hashValueSerializer()));
			}

			return result;
		}

		@Nullable
		abstract List<ByteMapRecord> inRedis(RedisConnection connection);
	}
}
