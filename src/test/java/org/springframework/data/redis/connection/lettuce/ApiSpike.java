package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.Limit;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.StatefulRedisConnection;
import lombok.Data;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.convert.RedisCustomConversions;
import org.springframework.data.redis.hash.HashMapper;
import org.springframework.data.redis.hash.ObjectHashMapper;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;
import org.springframework.util.NumberUtils;
import org.springframework.util.StringUtils;

/**
 * @author Christoph Strobl
 * @since 2018/10
 */
public class ApiSpike {

	RedisClient client;
	StatefulRedisConnection<byte[], byte[]> connection;

	LettuceConnection lc;

	@Before
	public void setUp() {

		client = RedisClient.create(RedisURI.create("localhost", 6379));
		lc = new LettuceConnection(1, client);
	}

	@After
	public void tearDown() {

		lc.flushAll();
		lc.close();
		client.shutdown();
	}

	@Test
	public void plainStuff() {

		RedisStreamCommandsImpl imp = new RedisStreamCommandsImpl(lc);

		StreamOperationsImpl<String, String, Object> ops = new StreamOperationsImpl<>(imp, RedisSerializer.string(),
				RedisSerializer.string(), RedisSerializer.java());

		EntryId id = ops.xAdd("foo", StreamEntry.of(Collections.singletonMap("field", "value")));
		List<MapStreamEntry<String, Object>> range = ops.xRange("foo", Range.unbounded());

		range.forEach(it -> System.out.println(it.getId() + ": " + it.asMap()));

		List<String> stringRange = ops.xRange("foo", Range.unbounded(), String.class);
		stringRange.forEach(System.out::println);
	}

	@Test
	public void simpleObject() {

		SimpleObject o = new SimpleObject();
		o.field1 = "value-1";
		o.field2 = 10L;

		RedisStreamCommandsImpl imp = new RedisStreamCommandsImpl(lc);

		StreamOperationsImpl<String, String, Object> ops = new StreamOperationsImpl<>(imp, RedisSerializer.string(),
				RedisSerializer.string(), RedisSerializer.java());

		EntryId id = ops.xAdd("key", o);
		List<SimpleObject> list = ops.xRange("key", Range.unbounded(), SimpleObject.class);

		list.forEach(System.out::println);

		// List<StreamEntry<String, Object>> range = ops.xRange("foo", Range.unbounded());
		//
		// range.forEach(it -> System.out.println(it.getId() + ": " + it.asMap()));
		//
		// List<String> stringRange = ops.forValue(Object.class).xRange("foo", Range.unbounded(), String.class);
		// stringRange.forEach(System.out::println);
	}

	@Test
	public void writeWithHashReadWithMap() {

		SimpleObject o = new SimpleObject();
		o.field1 = "value-1";
		o.field2 = 10L;

		RedisStreamCommandsImpl imp = new RedisStreamCommandsImpl(lc);

		StreamOperationsImpl<String, String, Object> ops = new StreamOperationsImpl<>(imp, RedisSerializer.string(),
				RedisSerializer.string(), RedisSerializer.java());

		EntryId id = ops.xAdd("key", o);

		List<MapStreamEntry<String, Object>> list = ops.xRange("key", Range.unbounded());

		list.forEach(System.out::println);

		// List<StreamEntry<String, Object>> range = ops.xRange("foo", Range.unbounded());
		//
		// range.forEach(it -> System.out.println(it.getId() + ": " + it.asMap()));
		//
		// List<String> stringRange = ops.forValue(Object.class).xRange("foo", Range.unbounded(), String.class);
		// stringRange.forEach(System.out::println);
	}

	@Data
	static class SimpleObject {

		String field1;
		Long field2;
	}

	@Test
	public void x2() {

		RedisCustomConversions rcc = new RedisCustomConversions();
		DefaultConversionService conversionService = new DefaultConversionService();
		rcc.registerConvertersIn(conversionService);

	}

	interface RedisStreamCommands {

		EntryId xAdd(byte[] key, MapStreamEntry<byte[], byte[]> entry);

		List<MapStreamEntry<byte[], byte[]>> xRange(byte[] key, Range<String> range);

	}

	static class RedisStreamCommandsImpl implements RedisStreamCommands {

		private LettuceConnection connection;

		public RedisStreamCommandsImpl(LettuceConnection connection) {
			this.connection = connection;
		}

		@Override
		public EntryId xAdd(byte[] key, MapStreamEntry<byte[], byte[]> entry) {
			return EntryId.of(connection.getConnection().xadd(key, entry.asMap()));
		}

		@Override
		public List<MapStreamEntry<byte[], byte[]>> xRange(byte[] key, Range<String> range) {

			List<StreamMessage<byte[], byte[]>> raw = connection.getConnection().xrange(key,
					io.lettuce.core.Range.unbounded(), Limit.unlimited());
			return raw.stream().map(it -> new RawEntry(EntryId.of(it.getId()), it.getBody())).collect(Collectors.toList());
		}
	}

	interface StreamOperations<K, HK, HV> {

		default EntryId xAdd(K key, Object value) {
			return xAdd(key, objectToEntry(value));
		}

		EntryId xAdd(K key, MapStreamEntry<HK, HV> entry);

		List<MapStreamEntry<HK, HV>> xRange(K key, Range<String> range);

		default <V> MapStreamEntry<HK, HV> objectToEntry(V value) {
			return StreamEntry.of(((HashMapper) getHashMapper(value.getClass())).toHash(value));
		}

		default <V> V entryToObject(MapStreamEntry<HK, HV> entry, Class<V> targetType) {
			return (V) getHashMapper(targetType).fromHash(entry.asMap());
		}

		default <V> List<V> xRange(K key, Range<String> range, Class<V> targetType) { // move to upper level and do not
			// allow change of HashMapper
			return xRange(key, range).stream().map(it -> entryToObject(it, targetType)).collect(Collectors.toList());
		}

		<V> HashMapper<V, HK, HV> getHashMapper(Class<V> targetType);
	}

	/*
	 * Conversion Rules
	 *
	 * 1) Simple types
	 *    serialize: default value serializer (key known byte array of class)
	 *    deserialized: default value serializer
	 * 2) Complex types
	 *    serialize: HashMapper: check if all entries are binary - then pass on to serializer
	 *    deserialize: deserialize then pass to hashMapper
	 *
	 *
	 *
	 *
	 */
	static class StreamOperationsImpl<K, HK, HV> implements StreamOperations<K, HK, HV> {

		private RedisSerializer<K> keySerializer;
		private RedisSerializer hashKeySerializer;
		private RedisSerializer hashValueSerializer;
		private RedisStreamCommands commands;
		private final RedisCustomConversions rcc = new RedisCustomConversions();
		private DefaultConversionService conversionService;

		private HashMapper<?, HK, HV> mapper;

		public StreamOperationsImpl(RedisStreamCommands commands, RedisSerializer<K> keySerializer,
				RedisSerializer<HK> hashKeySerializer, RedisSerializer<HV> hashValueSerializer) {

			this.commands = commands;
			this.keySerializer = keySerializer;
			this.hashKeySerializer = hashKeySerializer;
			this.hashValueSerializer = hashValueSerializer;

			this.conversionService = new DefaultConversionService();
			this.mapper = (HashMapper<?, HK, HV>) new ObjectHashMapper();
			rcc.registerConvertersIn(conversionService);
		}

		@Override
		public EntryId xAdd(K key, MapStreamEntry<HK, HV> entry) {
			return commands.xAdd(serializeKeyIfRequired(key), entry.map(this::mapToBinary));
		}

		@Override
		public List<MapStreamEntry<HK, HV>> xRange(K key, Range<String> range) {

			return commands.xRange(serializeKeyIfRequired(key), range).stream().map(it -> it.map(this::mapToObject))
					.collect(Collectors.toList());
		}

		@Override
		public <V> HashMapper<V, HK, HV> getHashMapper(Class<V> targetType) {

			if (rcc.isSimpleType(targetType)) {

				return new HashMapper<V, HK, HV>() {

					@Override
					public Map<HK, HV> toHash(V object) {
						return (Map<HK, HV>) Collections.singletonMap("payload".getBytes(StandardCharsets.UTF_8),
								serializeHashValueIfRequires((HV) object));
					}

					@Override
					public V fromHash(Map<HK, HV> hash) {
						Object value = hash.values().iterator().next();
						if (ClassUtils.isAssignableValue(targetType, value)) {
							return (V) value;
						}
						return (V) deserializeHashValue((byte[]) value, (Class<HV>) targetType);
					}
				};
			}

			if (mapper instanceof ObjectHashMapper) {

				return new HashMapper<V, HK, HV>() {

					@Override
					public Map<HK, HV> toHash(V object) {
						return (Map<HK, HV>) ((ObjectHashMapper) mapper).toObjectHash(object);
					}

					@Override
					public V fromHash(Map<HK, HV> hash) {

						Map<byte[], byte[]> map = hash.entrySet().stream()
								.collect(Collectors.toMap(e -> conversionService.convert((Object) e.getKey(), byte[].class),
										e -> conversionService.convert((Object) e.getValue(), byte[].class)));

						return (V) mapper.fromHash((Map<HK, HV>) map);
					}
				};

			}

			return (HashMapper<V, HK, HV>) mapper;
		}

		protected byte[] serializeHashKeyIfRequired(HK key) {

			return hashKeySerializerPresent() ? serialize(key, hashKeySerializer)
					: conversionService.convert(key, byte[].class);
		}

		protected boolean hashKeySerializerPresent() {
			return hashValueSerializer != null;
		}

		protected byte[] serializeHashValueIfRequires(HV value) {
			return hashValueSerializerPresent() ? serialize(value, hashValueSerializer)
					: conversionService.convert(value, byte[].class);
		}

		protected boolean hashValueSerializerPresent() {
			return hashValueSerializer != null;
		}

		protected byte[] serializeKeyIfRequired(K key) {
			return keySerializerPresent() ? serialize(key, keySerializer) : conversionService.convert(key, byte[].class);
		}

		protected boolean keySerializerPresent() {
			return keySerializer != null;
		}

		protected K deserializeKey(byte[] bytes, Class<K> targetType) {
			return keySerializerPresent() ? keySerializer.deserialize(bytes) : conversionService.convert(bytes, targetType);
		}

		protected HK deserializeHashKey(byte[] bytes, Class<HK> targetType) {

			return hashKeySerializerPresent() ? (HK) hashKeySerializer.deserialize(bytes)
					: conversionService.convert(bytes, targetType);
		}

		protected HV deserializeHashValue(byte[] bytes, Class<HV> targetType) {
			return hashValueSerializerPresent() ? (HV) hashValueSerializer.deserialize(bytes)
					: conversionService.convert(bytes, targetType);
		}

		byte[] serialize(Object value, RedisSerializer serializer) {

			Object _value = value;
			if (!serializer.canSerialize(value.getClass())) {
				_value = conversionService.convert(value, serializer.getTargetType());
			}
			return serializer.serialize(_value);
		}

		private Map.Entry<byte[], byte[]> mapToBinary(Map.Entry<HK, HV> it) {

			return new Map.Entry<byte[], byte[]>() {

				@Override
				public byte[] getKey() {
					return serializeHashKeyIfRequired(it.getKey());
				}

				@Override
				public byte[] getValue() {
					return serializeHashValueIfRequires(it.getValue());
				}

				@Override
				public byte[] setValue(byte[] value) {
					return new byte[0];
				}
			};
		}

		private Map.Entry<HK, HV> mapToObject(Map.Entry<byte[], byte[]> pair) {

			return new Map.Entry<HK, HV>() {

				@Override
				public HK getKey() {
					return deserializeHashKey(pair.getKey(), (Class<HK>) Object.class);
				}

				@Override
				public HV getValue() {
					return deserializeHashValue(pair.getValue(), (Class<HV>) Object.class);
				}

				@Override
				public HV setValue(HV value) {
					return value;
				}

			};
		}
	}

	static class EntryId {

		final String raw;

		public EntryId(String raw) {
			this.raw = raw;
		}

		static EntryId of(String value) {
			return new EntryId(value);
		}

		Long getTimestamp() {
			return value(0);
		}

		Long getSequence() {
			return value(1);
		}

		Long value(int index) {
			return NumberUtils.parseNumber(StringUtils.split(raw, "-")[index], Long.class);
		}

		@Override
		public String toString() {
			return raw;
		}
	}

	interface StreamEntry<V> extends Iterable<V> {

		@Nullable
		EntryId getId();

		<V1> StreamEntry<V1> map(Function<V, V1> mapFunction);

		static <K, V> MapStreamEntry<K, V> of(Map<K, V> map) {
			return new MapBackedStreamEntry<>(null, map);
		}
	}

	interface MapStreamEntry<K, V> extends Iterable<Map.Entry<K, V>> {

		@Nullable
		EntryId getId();

		Map<K, V> asMap();

		<K1, V1> MapStreamEntry<K1, V1> map(Function<Map.Entry<K, V>, Map.Entry<K1, V1>> mapFunction);
	}

	interface Pair<K, V> {

		K getField();

		V getValue();

	}

	static class MapBackedStreamEntry<K, V> implements MapStreamEntry<K, V> {

		private @Nullable EntryId entryId;
		private final Map<K, V> kvMap;

		MapBackedStreamEntry(@Nullable EntryId entryId, Map<K, V> kvMap) {

			this.entryId = entryId;
			this.kvMap = kvMap;
		}

		@Nullable
		@Override
		public EntryId getId() {
			return entryId;
		}

		@Override
		public <K1, V1> MapBackedStreamEntry<K1, V1> map(Function<Map.Entry<K, V>, Map.Entry<K1, V1>> mapFunction) {

			Map<K1, V1> mapped = new LinkedHashMap<>(kvMap.size(), 1);
			iterator().forEachRemaining(it -> {

				Map.Entry<K1, V1> mappedPair = mapFunction.apply(it);
				mapped.put(mappedPair.getKey(), mappedPair.getValue());
			});

			return new MapBackedStreamEntry<>(entryId, mapped);
		}

		@Override
		public Iterator<Map.Entry<K, V>> iterator() {
			return kvMap.entrySet().iterator();
		}

		@Override
		public Map<K, V> asMap() {
			return kvMap;
		}

		static <K, V> Pair<K, V> mapToPair(Entry<K, V> it) {

			return new Pair() {

				@Override
				public Object getField() {
					return it.getKey();
				}

				@Override
				public Object getValue() {
					return it.getValue();
				}
			};
		}

		@Override
		public String toString() {
			return "MapBackedStreamEntry{" + "entryId=" + entryId + ", kvMap=" + kvMap + '}';
		}

	}

	static class RawEntry extends MapBackedStreamEntry<byte[], byte[]> {

		RawEntry(@Nullable EntryId entryId, Map<byte[], byte[]> map) {
			super(entryId, map);
		}
	}

}
