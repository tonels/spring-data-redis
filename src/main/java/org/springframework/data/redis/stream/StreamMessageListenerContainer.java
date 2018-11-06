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
package org.springframework.data.redis.stream;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Predicate;

import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStreamCommands.Consumer;
import org.springframework.data.redis.connection.RedisStreamCommands.ReadOffset;
import org.springframework.data.redis.connection.RedisStreamCommands.Record;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.stream.DefaultStreamMessageListenerContainer.LoggingErrorHandler;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ErrorHandler;

/**
 * Abstraction used by the framework representing a message listener container. <strong>Not</strong> meant to be
 * implemented externally.
 * <p/>
 * Once created, a {@link StreamMessageListenerContainer} can subscribe to a Redis Stream and consume incoming
 * {@link Record messages}. {@link StreamMessageListenerContainer} allows multiple stream read requests and returns a
 * {@link Subscription} handle per read request. Cancelling the {@link Subscription} terminates eventually background
 * polling. Messages are converted using {@link RedisSerializer key and value serializers} to support various
 * serialization strategies. <br/>
 * {@link StreamMessageListenerContainer} supports multiple modes of stream consumption:
 * <ul>
 * <li>Standalone</li>
 * <li>Using a {@link Consumer} with external
 * {@link org.springframework.data.redis.core.StreamOperations#acknowledge(Object, String, String...)} acknowledge}</li>
 * <li>Using a {@link Consumer} with auto-acknowledge</li>
 * </ul>
 * Reading from a stream requires polling and a strategy to advance stream offsets. Depending on the initial
 * {@link ReadOffset}, {@link StreamMessageListenerContainer} applies an individual strategy to obtain the next
 * {@link ReadOffset}: <br/>
 * <strong>Standalone</strong>
 * <ul>
 * <li>{@link ReadOffset#from(String)} Offset using a particular message Id: Start with the given offset and use the
 * last seen {@link Record#getId() message Id}.</li>
 * <li>{@link ReadOffset#lastConsumed()} Last consumed: Start with the latest offset ({@code $}) and use the last seen
 * {@link Record#getId() message Id}.</li>
 * <li>{@link ReadOffset#latest()} Last consumed: Start with the latest offset ({@code $}) and use latest offset
 * ({@code $}) for subsequent reads.</li>
 * </ul>
 * <br/>
 * <strong>Using {@link Consumer}</strong>
 * <ul>
 * <li>{@link ReadOffset#from(String)} Offset using a particular message Id: Start with the given offset and use the
 * last seen {@link Record#getId() message Id}.</li>
 * <li>{@link ReadOffset#lastConsumed()} Last consumed: Start with the last consumed message by the consumer ({@code >})
 * and use the last consumed message by the consumer ({@code >}) for subsequent reads.</li>
 * <li>{@link ReadOffset#latest()} Last consumed: Start with the latest offset ({@code $}) and use latest offset
 * ({@code $}) for subsequent reads.</li>
 * </ul>
 * <strong>Note: Using {@link ReadOffset#latest()} bears the chance of dropped messages as messages can arrive in the
 * time during polling is suspended. Use messagedId's as offset or {@link ReadOffset#lastConsumed()} to minimize the
 * chance of message loss.</strong>
 * <p/>
 * {@link StreamMessageListenerContainer} requires a {@link Executor} to fork long-running polling tasks on a different
 * {@link Thread}. This thread is used as event loop to poll for stream messages and invoke the
 * {@link StreamListener#onMessage(Record) listener callback}.
 * <p/>
 * {@link StreamMessageListenerContainer} tasks propagate errors during stream reads and
 * {@link StreamListener#onMessage(Record) listener notification} to a configurable {@link ErrorHandler}. Errors stop a
 * {@link Subscription} by default. Configuring a {@link Predicate} for a {@link StreamReadRequest} allows conditional
 * subscription cancelling or continuing on all errors.
 * <p/>
 * See the following example code how to use {@link StreamMessageListenerContainer}:
 *
 * <pre class="code">
 * RedisConnectionFactory factory = …;
 *
 * StreamMessageListenerContainer<String, String> container = StreamMessageListenerContainer.create(factory);
 * Subscription subscription = container.receive(StreamOffset.fromStart("my-stream"), message -> …);
 *
 * container.start();
 *
 * // later
 * container.stop();
 * </pre>
 *
 * @author Mark Paluch
 * @param <K> Stream key and Stream field type.
 * @param <V> Stream value type.
 * @since 2.2
 * @see StreamMessageListenerContainerOptions#builder()
 * @see StreamListener
 * @see StreamReadRequest
 * @see ConsumerStreamReadRequest
 * @see StreamMessageListenerContainerOptionsBuilder#executor(Executor)
 * @see ErrorHandler
 * @see org.springframework.data.redis.core.StreamOperations
 * @see RedisConnectionFactory
 * @see StreamReceiver
 */
public interface StreamMessageListenerContainer<K, V> extends SmartLifecycle {

	/**
	 * Create a new {@link StreamMessageListenerContainer} using {@link StringRedisSerializer string serializers} given
	 * {@link RedisConnectionFactory}.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 * @return the new {@link StreamMessageListenerContainer}.
	 */
	static StreamMessageListenerContainer<String, Map<String, String>> create(RedisConnectionFactory connectionFactory) {

		Assert.notNull(connectionFactory, "RedisConnectionFactory must not be null!");

		return create(connectionFactory,
				StreamMessageListenerContainerOptions.builder().serializer(StringRedisSerializer.UTF_8).build());
	}

	/**
	 * Create a new {@link StreamMessageListenerContainer} given {@link RedisConnectionFactory} and
	 * {@link StreamMessageListenerContainerOptions}.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return the new {@link StreamMessageListenerContainer}.
	 */
	static <K, V> StreamMessageListenerContainer<K, V> create(RedisConnectionFactory connectionFactory,
			StreamMessageListenerContainerOptions<K, V> options) {

		Assert.notNull(connectionFactory, "RedisConnectionFactory must not be null!");
		Assert.notNull(options, "StreamMessageListenerContainerOptions must not be null!");

		return new DefaultStreamMessageListenerContainer<>(connectionFactory, options);
	}

	/**
	 * Register a new subscription for a Redis Stream. If the {@link StreamMessageListenerContainer#isRunning() is already
	 * running} the {@link Subscription} will be added and run immediately, otherwise it'll be scheduled and started once
	 * the container is actually {@link StreamMessageListenerContainer#start() started}.
	 * <p/>
	 * Errors during {@link org.springframework.data.redis.connection.RedisStreamCommands.StreamMessage} retrieval lead to
	 * {@link Subscription#cancel() cancellation} of the underlying task.
	 * <p/>
	 * On {@link StreamMessageListenerContainer#stop()} all {@link Subscription subscriptions} are cancelled prior to
	 * shutting down the container itself.
	 *
	 * @param streamOffset the stream along its offset.
	 * @param listener must not be {@literal null}.
	 * @return the subscription handle.
	 * @see StreamOffset#create(Object, ReadOffset)
	 */
	default Subscription receive(StreamOffset<K> streamOffset, StreamListener<K, V> listener) {
		return register(StreamReadRequest.builder(streamOffset).build(), listener);
	}

	/**
	 * Register a new subscription for a Redis Stream. If the {@link StreamMessageListenerContainer#isRunning() is already
	 * running} the {@link Subscription} will be added and run immediately, otherwise it'll be scheduled and started once
	 * the container is actually {@link StreamMessageListenerContainer#start() started}.
	 * <p/>
	 * Every message must be acknowledged using
	 * {@link org.springframework.data.redis.core.StreamOperations#acknowledge(Object, String, String...)} after
	 * processing.
	 * <p/>
	 * Errors during {@link org.springframework.data.redis.connection.RedisStreamCommands.StreamMessage} retrieval lead to
	 * {@link Subscription#cancel() cancellation} of the underlying task.
	 * <p/>
	 * On {@link StreamMessageListenerContainer#stop()} all {@link Subscription subscriptions} are cancelled prior to
	 * shutting down the container itself.
	 *
	 * @param consumer consumer group, must not be {@literal null}.
	 * @param streamOffset the stream along its offset.
	 * @param listener must not be {@literal null}.
	 * @return the subscription handle.
	 * @see StreamOffset#create(Object, ReadOffset)
	 * @see ReadOffset#lastConsumed()
	 */
	default Subscription receive(Consumer consumer, StreamOffset<K> streamOffset, StreamListener<K, V> listener) {
		return register(StreamReadRequest.builder(streamOffset).consumer(consumer).build(), listener);
	}

	/**
	 * Register a new subscription for a Redis Stream. If the {@link StreamMessageListenerContainer#isRunning() is already
	 * running} the {@link Subscription} will be added and run immediately, otherwise it'll be scheduled and started once
	 * the container is actually {@link StreamMessageListenerContainer#start() started}.
	 * <p/>
	 * Every message is acknowledged when received.
	 * <p/>
	 * Errors during {@link org.springframework.data.redis.connection.RedisStreamCommands.StreamMessage} retrieval lead to
	 * {@link Subscription#cancel() cancellation} of the underlying task.
	 * <p/>
	 * On {@link StreamMessageListenerContainer#stop()} all {@link Subscription subscriptions} are cancelled prior to
	 * shutting down the container itself.
	 *
	 * @param consumer consumer group, must not be {@literal null}.
	 * @param streamOffset the stream along its offset.
	 * @param listener must not be {@literal null}.
	 * @return the subscription handle.
	 * @see StreamOffset#create(Object, ReadOffset)
	 * @see ReadOffset#lastConsumed()
	 */
	default Subscription receiveAutoAck(Consumer consumer, StreamOffset<K> streamOffset, StreamListener<K, V> listener) {
		return register(StreamReadRequest.builder(streamOffset).consumer(consumer).autoAck(true).build(), listener);
	}

	/**
	 * Register a new subscription for a Redis Stream. If the {@link StreamMessageListenerContainer#isRunning() is already
	 * running} the {@link Subscription} will be added and run immediately, otherwise it'll be scheduled and started once
	 * the container is actually {@link StreamMessageListenerContainer#start() started}.
	 * <p/>
	 * Errors during {@link org.springframework.data.redis.connection.RedisStreamCommands.StreamMessage} are tested
	 * against test {@link StreamReadRequest#getCancelSubscriptionOnError() cancellation predicate} whether to cancel the
	 * underlying task.
	 * <p/>
	 * On {@link StreamMessageListenerContainer#stop()} all {@link Subscription subscriptions} are cancelled prior to
	 * shutting down the container itself.
	 * <p />
	 * Errors during {@link org.springframework.data.redis.connection.RedisStreamCommands.StreamMessage} retrieval are
	 * delegated to the given {@link StreamReadRequest#getErrorHandler()}.
	 *
	 * @param streamRequest must not be {@literal null}.
	 * @param listener must not be {@literal null}.
	 * @return the subscription handle.
	 * @see StreamReadRequest
	 * @see ConsumerStreamReadRequest
	 */
	Subscription register(StreamReadRequest<K> streamRequest, StreamListener<K, V> listener);

	/**
	 * Unregister a given {@link Subscription} from the container. This prevents the {@link Subscription} to be restarted
	 * in a potential {@link SmartLifecycle#stop() stop}/{@link SmartLifecycle#start() start} scenario.<br />
	 * An {@link Subscription#isActive() active} {@link Subscription subcription} is {@link Subscription#cancel()
	 * cancelled} prior to removal.
	 *
	 * @param subscription must not be {@literal null}.
	 */
	void remove(Subscription subscription);

	/**
	 * Request to read a Redis Stream.
	 *
	 * @param <K> Stream key and Stream field type.
	 * @see StreamReadRequestBuilder
	 */
	class StreamReadRequest<K> {

		private final StreamOffset<K> streamOffset;
		private final @Nullable ErrorHandler errorHandler;
		private final Predicate<Throwable> cancelSubscriptionOnError;

		private StreamReadRequest(StreamOffset<K> streamOffset, @Nullable ErrorHandler errorHandler,
				Predicate<Throwable> cancelSubscriptionOnError) {

			this.streamOffset = streamOffset;
			this.errorHandler = errorHandler;
			this.cancelSubscriptionOnError = cancelSubscriptionOnError;
		}

		/**
		 * @return a new builder for {@link StreamReadRequest}.
		 */
		static <K> StreamReadRequestBuilder<K> builder(StreamOffset<K> offset) {
			return new StreamReadRequestBuilder<>(offset);
		}

		public StreamOffset<K> getStreamOffset() {
			return streamOffset;
		}

		@Nullable
		public ErrorHandler getErrorHandler() {
			return errorHandler;
		}

		public Predicate<Throwable> getCancelSubscriptionOnError() {
			return cancelSubscriptionOnError;
		}
	}

	/**
	 * Request to read a Redis Stream with a {@link Consumer}.
	 *
	 * @param <K> Stream key and Stream field type.
	 * @see StreamReadRequestBuilder
	 */
	class ConsumerStreamReadRequest<K> extends StreamReadRequest<K> {

		private final Consumer consumer;
		private final boolean autoAck;

		private ConsumerStreamReadRequest(StreamOffset<K> streamOffset, @Nullable ErrorHandler errorHandler,
				Predicate<Throwable> cancelSubscriptionOnError, Consumer consumer, boolean autoAck) {
			super(streamOffset, errorHandler, cancelSubscriptionOnError);
			this.consumer = consumer;
			this.autoAck = autoAck;
		}

		public Consumer getConsumer() {
			return consumer;
		}

		public boolean isAutoAck() {
			return autoAck;
		}
	}

	/**
	 * Builder to build a {@link StreamReadRequest}.
	 *
	 * @param <K> Stream key and Stream field type.
	 */
	class StreamReadRequestBuilder<K> {

		final StreamOffset<K> streamOffset;
		@Nullable ErrorHandler errorHandler;
		Predicate<Throwable> cancelSubscriptionOnError = t -> true;

		StreamReadRequestBuilder(StreamOffset<K> streamOffset) {
			this.streamOffset = streamOffset;
		}

		StreamReadRequestBuilder(StreamReadRequestBuilder<K> other) {

			this.streamOffset = other.streamOffset;
			this.errorHandler = other.errorHandler;
			this.cancelSubscriptionOnError = other.cancelSubscriptionOnError;
		}

		/**
		 * Configure a {@link ErrorHandler} to be notified on {@link Throwable errors}.
		 *
		 * @param errorHandler must not be null.
		 * @return {@code this} {@link StreamReadRequestBuilder}.
		 */
		public StreamReadRequestBuilder<K> errorHandler(ErrorHandler errorHandler) {

			this.errorHandler = errorHandler;
			return this;
		}

		/**
		 * Configure a cancellation {@link Predicate} to be notified on {@link Throwable errors}. The outcome of the
		 * {@link Predicate} decides whether to cancel the subscription by returning {@literal true}.
		 *
		 * @param cancelSubscriptionOnError must not be null.
		 * @return {@code this} {@link StreamReadRequestBuilder}.
		 */
		public StreamReadRequestBuilder<K> cancelOnError(Predicate<Throwable> cancelSubscriptionOnError) {

			this.cancelSubscriptionOnError = cancelSubscriptionOnError;
			return this;
		}

		/**
		 * Configure a {@link Consumer} to consume stream messages within a consumer group.
		 *
		 * @param consumer must not be null.
		 * @return a new {@link ConsumerStreamReadRequestBuilder}.
		 */
		public ConsumerStreamReadRequestBuilder<K> consumer(Consumer consumer) {
			return new ConsumerStreamReadRequestBuilder<>(this).consumer(consumer);
		}

		/**
		 * Build a new instance of {@link StreamReadRequest}.
		 *
		 * @return a new instance of {@link StreamReadRequest}.
		 */
		public StreamReadRequest<K> build() {
			return new StreamReadRequest<>(streamOffset, errorHandler, cancelSubscriptionOnError);
		}
	}

	/**
	 * Builder to build a {@link ConsumerStreamReadRequest}.
	 *
	 * @param <K> Stream key and Stream field type.
	 */
	class ConsumerStreamReadRequestBuilder<K> extends StreamReadRequestBuilder<K> {

		private Consumer consumer;
		private boolean autoAck = true;

		ConsumerStreamReadRequestBuilder(StreamReadRequestBuilder<K> other) {
			super(other);
		}

		/**
		 * Configure a {@link ErrorHandler} to be notified on {@link Throwable errors}.
		 *
		 * @param errorHandler must not be null.
		 * @return {@code this} {@link ConsumerStreamReadRequestBuilder}.
		 */
		public ConsumerStreamReadRequestBuilder<K> errorHandler(ErrorHandler errorHandler) {

			super.errorHandler(errorHandler);
			return this;
		}

		/**
		 * Configure a cancellation {@link Predicate} to be notified on {@link Throwable errors}. The outcome of the
		 * {@link Predicate} decides whether to cancel the subscription by returning {@literal true}.
		 *
		 * @param cancelSubscriptionOnError must not be null.
		 * @return {@code this} {@link ConsumerStreamReadRequestBuilder}.
		 */
		public ConsumerStreamReadRequestBuilder<K> cancelOnError(Predicate<Throwable> cancelSubscriptionOnError) {

			super.cancelOnError(cancelSubscriptionOnError);
			return this;
		}

		/**
		 * Configure a {@link Consumer} to consume stream messages within a consumer group.
		 *
		 * @param consumer must not be null.
		 * @return {@code this} {@link ConsumerStreamReadRequestBuilder}.
		 */
		public ConsumerStreamReadRequestBuilder<K> consumer(Consumer consumer) {

			this.consumer = consumer;
			return this;
		}

		/**
		 * Configure auto-acknowledgement for stream message consumption.
		 *
		 * @param autoAck {@literal true} (default) to auto-acknowledge received messages or {@literal false} for external
		 *          acknowledgement.
		 * @return {@code this} {@link ConsumerStreamReadRequestBuilder}.
		 */
		public ConsumerStreamReadRequestBuilder<K> autoAck(boolean autoAck) {

			this.autoAck = autoAck;
			return this;
		}

		/**
		 * Build a new instance of {@link ConsumerStreamReadRequest}.
		 *
		 * @return a new instance of {@link ConsumerStreamReadRequest}.
		 */
		public ConsumerStreamReadRequest<K> build() {
			return new ConsumerStreamReadRequest<>(streamOffset, errorHandler, cancelSubscriptionOnError, consumer, autoAck);
		}
	}

	/**
	 * Options for {@link StreamMessageListenerContainer}.
	 *
	 * @param <K> Stream key and Stream field type.
	 * @param <V> Stream value type.
	 * @see StreamMessageListenerContainerOptionsBuilder
	 */
	@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
	class StreamMessageListenerContainerOptions<K, V> {

		private final Duration pollTimeout;
		private final int batchSize;
		private final RedisSerializer<K> keySerializer;
		private final RedisSerializer<V> bodySerializer;
		private final ErrorHandler errorHandler;
		private final Executor executor;

		/**
		 * @return a new builder for {@link StreamMessageListenerContainerOptions}.
		 */
		static StreamMessageListenerContainerOptionsBuilder<String, Map<String, String>> builder() {
			return new StreamMessageListenerContainerOptionsBuilder<>().serializer(StringRedisSerializer.UTF_8);
		}

		/**
		 * Timeout for blocking polling using the {@code BLOCK} option during reads.
		 *
		 * @return
		 */
		public Duration getPollTimeout() {
			return pollTimeout;
		}

		/**
		 * Batch size polling using the {@code COUNT} option during reads.
		 *
		 * @return
		 */
		public int getBatchSize() {
			return batchSize;
		}

		public RedisSerializer<K> getKeySerializer() {
			return keySerializer;
		}

		public RedisSerializer<V> getBodySerializer() {
			return bodySerializer;
		}

		/**
		 * @return the default {@link ErrorHandler}.
		 */
		public ErrorHandler getErrorHandler() {
			return errorHandler;
		}

		/**
		 * @return the {@link Executor} to run stream polling {@link Task}s. Defaults to {@link SimpleAsyncTaskExecutor}.
		 */
		public Executor getExecutor() {
			return executor;
		}
	}

	/**
	 * Builder for {@link StreamMessageListenerContainerOptions}.
	 *
	 * @param <K> Stream key and Stream field type
	 * @param <V> Stream value type
	 */
	class StreamMessageListenerContainerOptionsBuilder<K, V> {

		private Duration pollTimeout = Duration.ofSeconds(2);
		private int batchSize = 1;
		private RedisSerializer<K> keySerializer;
		private RedisSerializer<V> bodySerializer;
		private ErrorHandler errorHandler = LoggingErrorHandler.INSTANCE;
		private Executor executor = new SimpleAsyncTaskExecutor();

		private StreamMessageListenerContainerOptionsBuilder() {}

		/**
		 * Configure a poll timeout for the {@code BLOCK} option during reading.
		 *
		 * @param pollTimeout must not be {@literal null} or negative.
		 * @return {@code this} {@link StreamMessageListenerContainerOptionsBuilder}.
		 */
		public StreamMessageListenerContainerOptionsBuilder<K, V> pollTimeout(Duration pollTimeout) {

			Assert.notNull(pollTimeout, "Poll timeout must not be null!");
			Assert.isTrue(!pollTimeout.isNegative(), "Poll timeout must not be negative!");

			this.pollTimeout = pollTimeout;
			return this;
		}

		/**
		 * Configure a batch size for the {@code COUNT} option during reading.
		 *
		 * @param messagesPerPoll must not be greater zero.
		 * @return {@code this} {@link StreamMessageListenerContainerOptionsBuilder}.
		 */
		public StreamMessageListenerContainerOptionsBuilder<K, V> batchSize(int messagesPerPoll) {

			Assert.isTrue(messagesPerPoll > 0, "Batch size must be greater zero!");

			this.batchSize = messagesPerPoll;
			return this;
		}

		/**
		 * Configure a {@link Executor} to run stream polling {@link Task}s.
		 *
		 * @param executor must not be null.
		 * @return {@code this} {@link StreamMessageListenerContainerOptionsBuilder}.
		 */
		public StreamMessageListenerContainerOptionsBuilder<K, V> executor(Executor executor) {

			Assert.notNull(executor, "Executor must not be null!");

			this.executor = executor;
			return this;
		}

		/**
		 * Configure a {@link ErrorHandler} to be notified on {@link Throwable errors}.
		 *
		 * @param errorHandler must not be null.
		 * @return {@code this} {@link StreamMessageListenerContainerOptionsBuilder}.
		 */
		public StreamMessageListenerContainerOptionsBuilder<K, V> errorHandler(ErrorHandler errorHandler) {

			Assert.notNull(errorHandler, "ErrorHandler must not be null!");

			this.errorHandler = errorHandler;
			return this;
		}

		/**
		 * Configure a key and value serializer.
		 *
		 * @param serializer must not be {@literal null}.
		 * @return {@code this} {@link StreamMessageListenerContainerOptionsBuilder}.
		 */
		public <T> StreamMessageListenerContainerOptionsBuilder<T, Map<T, T>> serializer(RedisSerializer<T> serializer) {

			this.keySerializer = (RedisSerializer) serializer;
			this.bodySerializer = (RedisSerializer) serializer;
			return (StreamMessageListenerContainerOptionsBuilder) this;
		}

		/**
		 * Configure a key serializer.
		 *
		 * @param serializer must not be {@literal null}.
		 * @return {@code this} {@link StreamMessageListenerContainerOptionsBuilder}.
		 */
		public <NK> StreamMessageListenerContainerOptionsBuilder<NK, V> keySerializer(RedisSerializer<NK> serializer) {

			this.keySerializer = (RedisSerializer) serializer;
			return (StreamMessageListenerContainerOptionsBuilder) this;
		}

		/**
		 * Configure a value serializer.
		 *
		 * @param serializer must not be {@literal null}.
		 * @return {@code this} {@link StreamMessageListenerContainerOptionsBuilder}.
		 */
		public <NV> StreamMessageListenerContainerOptionsBuilder<K, NV> bodySerializer(RedisSerializer<NV> serializer) {

			this.bodySerializer = (RedisSerializer) serializer;
			return (StreamMessageListenerContainerOptionsBuilder) this;
		}

		/**
		 * Build new {@link StreamMessageListenerContainerOptions}.
		 *
		 * @return new {@link StreamMessageListenerContainerOptions}.
		 */
		public StreamMessageListenerContainerOptions<K, V> build() {
			return new StreamMessageListenerContainerOptions<>(pollTimeout, batchSize, keySerializer, bodySerializer,
					errorHandler, executor);
		}
	}
}
