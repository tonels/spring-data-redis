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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.Range;
import io.lettuce.core.Range.Boundary;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.protocol.LettuceCharsets;

import java.nio.ByteBuffer;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
class ArgumentConverters {

	static <T> Range<T> toRange(org.springframework.data.domain.Range<?> range) {
		return toRange(range, true);
	}

	static <T> Range<T> toRange(org.springframework.data.domain.Range<?> range, boolean applyEncoding) {
		return Range.from(lowerBoundArgOf(range, applyEncoding), upperBoundArgOf(range, applyEncoding));
	}

	@SuppressWarnings("unchecked")
	static <T> Boundary<T> lowerBoundArgOf(org.springframework.data.domain.Range<?> range, boolean applyEncoding) {
		return (Boundary<T>) rangeToBoundArgumentConverter(false, applyEncoding).convert(range);
	}

	@SuppressWarnings("unchecked")
	static <T> Boundary<T> upperBoundArgOf(org.springframework.data.domain.Range<?> range, boolean applyEncoding) {
		return (Boundary<T>) rangeToBoundArgumentConverter(true, applyEncoding).convert(range);
	}

	private static Converter<org.springframework.data.domain.Range<?>, Boundary<?>> rangeToBoundArgumentConverter(
			boolean upper, boolean applyEncoding) {

		return (source) -> {

			Boolean inclusive = upper ? source.getUpperBound().isInclusive() : source.getLowerBound().isInclusive();
			Object value = upper ? source.getUpperBound().getValue().orElse(null)
					: source.getLowerBound().getValue().orElse(null);

			if (value instanceof Number) {
				return inclusive ? Boundary.including((Number) value) : Boundary.excluding((Number) value);
			}

			if (value instanceof String) {

				StringCodec stringCodec = new StringCodec(LettuceCharsets.UTF8);
				if (!StringUtils.hasText((String) value) || ObjectUtils.nullSafeEquals(value, "+")
						|| ObjectUtils.nullSafeEquals(value, "-")) {
					return Boundary.unbounded();
				}

				if (applyEncoding) {
					return inclusive ? Boundary.including(stringCodec.encodeValue((String) value))
						: Boundary.excluding(stringCodec.encodeValue((String) value));
				}

				return inclusive ? Boundary.including(value) : Boundary.excluding(value);
			}

			if (value == null) {
				return upper ? Boundary.including("+") : Boundary.including("-");
			}

			return inclusive ? Boundary.including((ByteBuffer) value) : Boundary.excluding((ByteBuffer) value);
		};
	}

	static XReadArgs toReadArgs(StreamReadOptions readOptions) {

		XReadArgs args = new XReadArgs();

		if (readOptions.isNoack()) {
			args.noack(true);
		}

		if (readOptions.getBlock() != null) {
			args.block(readOptions.getBlock());
		}

		if (readOptions.getCount() != null) {
			args.count(readOptions.getCount());
		}
		return args;
	}
}
