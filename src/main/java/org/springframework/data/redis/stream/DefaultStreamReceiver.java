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

import lombok.RequiredArgsConstructor;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Operators;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.reactivestreams.Subscription;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair;

/**
 * Default implementation of {@link StreamReceiver}.
 *
 * @author Mark Paluch
 * @since 2.2
 */
class DefaultStreamReceiver<K, HK, HV> implements StreamReceiver<K, HK, HV> {

	private final Log logger = LogFactory.getLog(getClass());
	private final ReactiveRedisTemplate<K, ?> template;
	private final StreamReadOptions readOptions;

	/**
	 * Create a new {@link DefaultStreamReceiver} given {@link ReactiveRedisConnectionFactory} and
	 * {@link org.springframework.data.redis.stream.StreamReceiver.StreamReceiverOptions}.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 */
	@SuppressWarnings("unchecked")
	DefaultStreamReceiver(ReactiveRedisConnectionFactory connectionFactory, StreamReceiverOptions<K, HK, HV> options) {

		RedisSerializationContext<HK, HV> serializationContext = RedisSerializationContext
				.<HK, HV> newSerializationContext(options.getKeySerializer()) //
				.key((SerializationPair<HK>) options.getKeySerializer()) //
				.value((SerializationPair<HV>) options.getBodySerializer()) //
				.build();

		StreamReadOptions readOptions = StreamReadOptions.empty().count(options.getBatchSize());
		if (!options.getPollTimeout().isZero()) {
			readOptions = readOptions.block(options.getPollTimeout());
		}

		this.readOptions = readOptions;
		this.template = new ReactiveRedisTemplate(connectionFactory, serializationContext);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.stream.StreamReceiver#receive(org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Flux<MapRecord<K, HK, HV>> receive(StreamOffset<K> streamOffset) {

		if (logger.isDebugEnabled()) {
			logger.debug(String.format("receive(%s)", streamOffset));
		}

		return Flux.defer(() -> {

			PollState pollState = PollState.standalone(streamOffset.getOffset());
			BiFunction<K, ReadOffset, Flux<MapRecord<K, HK, HV>>> readFunction = (key, readOffset) -> template
					.<HK, HV> opsForStream().read(readOptions, StreamOffset.create(key, readOffset));

			return Flux.create(sink -> new StreamSubscription(sink, streamOffset.getKey(), pollState, readFunction).arm());
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.stream.StreamReceiver#receiveAutoAck(org.springframework.data.redis.connection.RedisStreamCommands.Consumer, org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Flux<MapRecord<K, HK, HV>> receiveAutoAck(Consumer consumer, StreamOffset<K> streamOffset) {

		if (logger.isDebugEnabled()) {
			logger.debug(String.format("receiveAutoAck(%s, %s)", consumer, streamOffset));
		}

		return Flux.defer(() -> {

			PollState pollState = PollState.consumer(consumer, streamOffset.getOffset());
			BiFunction<K, ReadOffset, Flux<MapRecord<K, HK, HV>>> readFunction = (key, readOffset) -> template
					.<HK, HV> opsForStream().read(consumer, readOptions, StreamOffset.create(key, readOffset));

			return Flux.create(sink -> new StreamSubscription(sink, streamOffset.getKey(), pollState, readFunction).arm());
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.stream.StreamReceiver#receive(org.springframework.data.redis.connection.RedisStreamCommands.Consumer, org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Flux<MapRecord<K, HK, HV>> receive(Consumer consumer, StreamOffset<K> streamOffset) {

		if (logger.isDebugEnabled()) {
			logger.debug(String.format("receive(%s, %s)", consumer, streamOffset));
		}

		StreamReadOptions noack = readOptions.noack();

		return Flux.defer(() -> {

			PollState pollState = PollState.consumer(consumer, streamOffset.getOffset());
			BiFunction<K, ReadOffset, Flux<MapRecord<K, HK, HV>>> readFunction = (key, readOffset) -> template
					.<HK, HV> opsForStream().read(consumer, noack, StreamOffset.create(key, readOffset));

			return Flux.create(sink -> new StreamSubscription(sink, streamOffset.getKey(), pollState, readFunction).arm());
		});
	}

	/**
	 * A stateful Redis Stream subscription.
	 */
	@RequiredArgsConstructor
	class StreamSubscription {

		private final Queue<MapRecord<K, HK, HV>> overflow = Queues.<MapRecord<K, HK, HV>> small().get();

		private final FluxSink<MapRecord<K, HK, HV>> sink;
		private final K key;
		private final PollState pollState;
		private final BiFunction<K, ReadOffset, Flux<MapRecord<K, HK, HV>>> readFunction;

		/**
		 * Arm the subscription so {@link Subscription#request(long) demand} activates polling.
		 *
		 * @return
		 */
		void arm() {

			sink.onRequest(toAdd -> {

				if (logger.isDebugEnabled()) {
					logger.debug(String.format("[stream: %s] onRequest(%d)", key, toAdd));
				}

				if (pollState.isSubscriptionActive()) {

					long r, u;
					for (;;) {
						r = pollState.getRequested();
						if (r == Long.MAX_VALUE) {
							scheduleIfRequired();
							return;
						}
						u = Operators.addCap(r, toAdd);
						if (pollState.setRequested(r, u)) {
							if (u > 0) {
								scheduleIfRequired();
							}
							return;
						}
					}
				} else {
					if (logger.isDebugEnabled()) {
						logger.debug(String.format("[stream: %s] onRequest(%d): Dropping, subscription canceled", key, toAdd));
					}
				}
			});

			sink.onCancel(pollState::cancel);
		}

		private void scheduleIfRequired() {

			if (logger.isDebugEnabled()) {
				logger.debug(String.format("[stream: %s] scheduleIfRequired()", key));
			}
			if (pollState.isScheduled()) {
				if (logger.isDebugEnabled()) {
					logger.debug(String.format("[stream: %s] scheduleIfRequired(): Already scheduled", key));
				}
				return;
			}

			if (!pollState.isSubscriptionActive()) {
				if (logger.isDebugEnabled()) {
					logger.debug(String.format("[stream: %s] scheduleIfRequired(): Subscription cancelled", key));
				}
				return;
			}

			if (pollState.getRequested() > 0 && !overflow.isEmpty()) {
				if (logger.isDebugEnabled()) {
					logger.info(String.format("[stream: %s] scheduleIfRequired(): Requested: %d Emit from buffer", key,
							pollState.getRequested()));
				}
				emitBuffer();
			}

			if (pollState.getRequested() == 0) {

				if (logger.isDebugEnabled()) {
					logger.debug(String
							.format("[stream: %s] scheduleIfRequired(): Subscriber has no demand. Suspending subscription.", key));
				}
				return;
			}

			if (pollState.getRequested() <= 0) {
				return;
			}

			if (pollState.activateSchedule()) {

				if (logger.isDebugEnabled()) {
					logger.debug(String.format("[stream: %s] scheduleIfRequired(): Activating subscription", key));
				}

				ReadOffset readOffset = pollState.getCurrentReadOffset();

				if (logger.isDebugEnabled()) {
					logger.debug(
							String.format("[stream: %s] scheduleIfRequired(): Activating subscription, offset %s", key, readOffset));
				}

				Flux<MapRecord<K, HK, HV>> poll = readFunction.apply(key, readOffset);

				poll.subscribe(getSubscriber());
			}
		}

		private CoreSubscriber<MapRecord<K, HK, HV>> getSubscriber() {

			return new CoreSubscriber<MapRecord<K, HK, HV>>() {

				@Override
				public void onSubscribe(Subscription s) {
					s.request(Long.MAX_VALUE);
				}

				@Override
				public void onNext(MapRecord<K, HK, HV> message) {
					onStreamMessage(message);
				}

				@Override
				public void onError(Throwable t) {
					onStreamError(t);
				}

				@Override
				public void onComplete() {

					if (logger.isDebugEnabled()) {
						logger.debug(String.format("[stream: %s] onComplete()", key));
					}

					pollState.scheduleCompleted();

					scheduleIfRequired();
				}

				@Override
				public Context currentContext() {
					return sink.currentContext();
				}
			};
		}

		private void onStreamMessage(MapRecord<K, HK, HV> message) {

			if (logger.isDebugEnabled()) {
				logger.debug(String.format("[stream: %s] onStreamMessage(%s)", key, message));
			}

			pollState.updateReadOffset(message.getId().getValue());

			long requested = pollState.getRequested();

			if (requested > 0) {

				if (requested == Long.MAX_VALUE) {

					if (logger.isDebugEnabled()) {
						logger.debug(String.format("[stream: %s] onStreamMessage(%s): Emitting item, fast-path", key, message));
					}
					sink.next(message);
				} else {

					if (pollState.decrementRequested()) {
						if (logger.isDebugEnabled()) {
							logger.debug(String.format("[stream: %s] onStreamMessage(%s): Emitting item, slow-path", key, message));
						}
						sink.next(message);
					} else {

						if (logger.isDebugEnabled()) {
							logger.debug(String.format("[stream: %s] onStreamMessage(%s): Buffering overflow", key, message));
						}
						overflow.add(message);
					}
				}

			} else {

				if (logger.isDebugEnabled()) {
					logger.debug(String.format("[stream: %s] onStreamMessage(%s): Buffering overflow", key, message));
				}
				overflow.offer(message);
			}
		}

		private void onStreamError(Throwable t) {

			if (logger.isDebugEnabled()) {
				logger.debug(String.format("[stream: %s] onStreamError(%s)", key, t));
			}

			pollState.cancel();
			sink.error(t);
		}

		private void emitBuffer() {

			while (!overflow.isEmpty()) {

				long demand = pollState.getRequested();

				if (demand <= 0) {
					break;
				}

				if (demand == Long.MAX_VALUE) {

					MapRecord<K, HK, HV> message = overflow.poll();

					if (message == null) {
						if (logger.isDebugEnabled()) {
							logger.debug(String.format("[stream: %s] emitBuffer(): emission missed", key));
						}
						break;
					}

					if (logger.isDebugEnabled()) {
						logger.debug(
								String.format("[stream: %s] emitBuffer(%s): Emitting item from buffer, fast-path", key, message));
					}

					sink.next(message);

				} else if (pollState.setRequested(demand, demand - 1)) {

					MapRecord<K, HK, HV> message = overflow.poll();

					if (message == null) {

						if (logger.isDebugEnabled()) {
							logger.debug(String.format("[stream: %s] emitBuffer(): emission missed", key));
						}
						pollState.incrementRequested();
						break;
					}

					if (logger.isDebugEnabled()) {
						logger.debug(
								String.format("[stream: %s] emitBuffer(%s): Emitting item from buffer, slow-path", key, message));
					}

					sink.next(message);
				}
			}
		}
	}

	/**
	 * Object representing the current polling state for a particular stream subscription.
	 */
	static class PollState {

		private final AtomicLong requestsPending = new AtomicLong();
		private final AtomicBoolean active = new AtomicBoolean(true);
		private final AtomicBoolean scheduled = new AtomicBoolean(false);

		private final ReadOffsetStrategy readOffsetStrategy;
		private final AtomicReference<ReadOffset> currentOffset;
		private final Optional<Consumer> consumer;

		private PollState(Optional<Consumer> consumer, ReadOffsetStrategy readOffsetStrategy, ReadOffset currentOffset) {

			this.readOffsetStrategy = readOffsetStrategy;
			this.currentOffset = new AtomicReference<>(currentOffset);
			this.consumer = consumer;
		}

		/**
		 * Create a new state object for standalone-read.
		 *
		 * @param offset
		 * @return
		 */
		static PollState standalone(ReadOffset offset) {

			ReadOffsetStrategy strategy = ReadOffsetStrategy.getStrategy(offset);
			return new PollState(Optional.empty(), strategy, strategy.getFirst(offset, Optional.empty()));
		}

		/**
		 * Create a new state object for consumergroup-read.
		 *
		 * @param consumer
		 * @param offset
		 * @return
		 */
		static PollState consumer(Consumer consumer, ReadOffset offset) {

			ReadOffsetStrategy strategy = ReadOffsetStrategy.getStrategy(offset);
			Optional<Consumer> optionalConsumer = Optional.of(consumer);
			return new PollState(optionalConsumer, strategy, strategy.getFirst(offset, optionalConsumer));
		}

		/**
		 * @return {@literal true} if the subscription is active.
		 */
		public boolean isSubscriptionActive() {
			return active.get();
		}

		/**
		 * Cancel the subscription.
		 */
		public void cancel() {
			active.set(false);
		}

		/**
		 * Decrement request count to indicate that an element was emitted.
		 *
		 * @return
		 */
		boolean decrementRequested() {

			long demand = requestsPending.get();

			if (demand > 0) {
				return requestsPending.compareAndSet(demand, demand - 1);
			}

			return false;
		}

		/**
		 * Increment request count.
		 */
		void incrementRequested() {
			requestsPending.incrementAndGet();
		}

		/**
		 * @return the number of requested items.
		 */
		public long getRequested() {
			return requestsPending.get();
		}

		/**
		 * Update demand.
		 *
		 * @param expect
		 * @param update
		 * @return
		 */
		boolean setRequested(long expect, long update) {
			return requestsPending.compareAndSet(expect, update);
		}

		/**
		 * Activate the schedule and return the synchronization state.
		 *
		 * @return {@literal true} if the schedule was activated by this call or {@literal false} if a different process
		 *         activated the schedule.
		 */
		boolean activateSchedule() {
			return scheduled.compareAndSet(false, true);
		}

		/**
		 * @return {@literal true} if the schedule is activated.
		 */
		public boolean isScheduled() {
			return scheduled.get();
		}

		/**
		 * Deactivate the schedule.
		 */
		void scheduleCompleted() {
			scheduled.compareAndSet(true, false);
		}

		/**
		 * Advance the {@link ReadOffset}.
		 */
		void updateReadOffset(String messageId) {

			ReadOffset next = readOffsetStrategy.getNext(getCurrentReadOffset(), consumer, messageId);
			this.currentOffset.set(next);
		}

		ReadOffset getCurrentReadOffset() {
			return currentOffset.get();
		}
	}
}
