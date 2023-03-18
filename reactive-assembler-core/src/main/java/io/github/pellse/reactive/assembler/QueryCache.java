/*
 * Copyright 2023 Sebastien Pelletier
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

package io.github.pellse.reactive.assembler;

import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Supplier;

@FunctionalInterface
public interface QueryCache<ID, R> extends BiFunction<Iterable<ID>, Rule<ID, R>, Mono<Map<ID, R>>> {

    static <ID, R> Rule<ID, R> cached(Rule<ID, R> rule) {
        return cached(rule, defaultCache());
    }

    static <ID, R> Rule<ID, R> cached(Rule<ID, R> rule, QueryCache<ID, R> cache) {
        return cached(rule, cache, null);
    }

    static <ID, R> Rule<ID, R> cached(Rule<ID, R> rule, Duration ttl) {
        return cached(rule, defaultCache(), ttl);
    }

    static <ID, R> Rule<ID, R> cached(Rule<ID, R> rule, QueryCache<ID, R> cache, Duration ttl) {
        return entityIds -> cache.apply(entityIds, ids -> toCachedMono(rule.apply(ids), ttl));
    }

    static <ID, R> QueryCache<ID, R> cache(Supplier<Map<Iterable<ID>, Mono<Map<ID, R>>>> mapSupplier) {
        return cache(mapSupplier.get());
    }

    static <ID, R> QueryCache<ID, R> cache(Map<Iterable<ID>, Mono<Map<ID, R>>> map) {
        return map::computeIfAbsent;
    }

    private static <T> Mono<T> toCachedMono(Mono<T> mono, Duration ttl) {
        return ttl != null ? mono.cache(ttl) : mono.cache();
    }

    private static <ID, R> QueryCache<ID, R> defaultCache() {
        return cache(ConcurrentHashMap::new);
    }
}
