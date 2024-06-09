/*
 * Copyright 2024 Sebastien Pelletier
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.pellse.assembler.caching;

import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.function.Function.identity;

public sealed interface CacheEvent<R> {

    static <R> Updated<R> updated(R value) {
        return new Updated<>(value);
    }

    static <R> Removed<R> removed(R value) {
        return new Removed<>(value);
    }

    static <T> Function<T, CacheEvent<T>> toCacheEvent(Predicate<T> isAddOrUpdateEvent) {
        return toCacheEvent(isAddOrUpdateEvent, identity());
    }

    static <T, R> Function<T, CacheEvent<R>> toCacheEvent(Predicate<T> isAddOrUpdateEvent, Function<T, R> cacheEventValueExtractor) {
        return source -> toCacheEvent(isAddOrUpdateEvent.test(source), cacheEventValueExtractor.apply(source));
    }

    static <R> CacheEvent<R> toCacheEvent(boolean isAddOrUpdateEvent, R eventValue) {
        return isAddOrUpdateEvent ? updated(eventValue) : removed(eventValue);
    }

    R value();

    record Updated<R>(R value) implements CacheEvent<R> {
    }

    record Removed<R>(R value) implements CacheEvent<R> {
    }
}


