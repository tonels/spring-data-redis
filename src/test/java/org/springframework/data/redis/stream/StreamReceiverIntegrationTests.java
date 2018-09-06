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

import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import org.springframework.data.redis.connection.RedisStreamCommands.Consumer;
import org.springframework.data.redis.connection.RedisStreamCommands.ReadOffset;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamMessage;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.stream.StreamReceiver.StreamReceiverOptions;

/**
 * @author Mark Paluch
 */
public class StreamReceiverIntegrationTests {

	public void demo() throws Exception {

		LettuceConnectionFactory factory = new LettuceConnectionFactory();
		factory.afterPropertiesSet();

		StreamReceiverOptions<String, String> options = StreamReceiverOptions.builder().batchSize(1).build();

		StreamReceiver<String, String> receiver = StreamReceiver.create(factory);
		Flux<StreamMessage<String, String>> messageFlux = receiver.receive(Consumer.from("foo", "bar"),
				StreamOffset.create("my-stream", ReadOffset.from("0-0")));

		Disposable subscribe = messageFlux.doOnNext(System.out::println).subscribe();
		System.in.read();
	}
}
