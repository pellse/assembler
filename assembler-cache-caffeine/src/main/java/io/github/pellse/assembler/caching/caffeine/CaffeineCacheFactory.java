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

package io.github.pellse.assembler.caching.caffeine;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.pellse.assembler.caching.Cache;
import io.github.pellse.assembler.caching.factory.CacheContext;
import io.github.pellse.assembler.caching.factory.CacheFactory;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;

import static com.github.benmanes.caffeine.cache.Caffeine.newBuilder;
import static io.github.pellse.assembler.caching.factory.CacheFactory.toMono;
import static io.github.pellse.util.ObjectUtils.also;
import static java.util.Map.of;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static reactor.core.publisher.Mono.fromFuture;

public interface CaffeineCacheFactory {

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheFactory<ID, R, RRC, CTX> caffeineCache() {
        return caffeineCache(defaultBuilder());
    }

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheFactory<ID, R, RRC, CTX> caffeineCache(long maxSize) {
        return caffeineCache(defaultBuilder()
                .maximumSize(maxSize));
    }

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheFactory<ID, R, RRC, CTX> caffeineCache(Duration expireAfterAccessDuration) {
        return caffeineCache(defaultBuilder()
                .expireAfterAccess(expireAfterAccessDuration));
    }

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheFactory<ID, R, RRC, CTX> caffeineCache(long maxSize, Duration expireAfterAccessDuration) {
        return caffeineCache(defaultBuilder()
                .maximumSize(maxSize)
                .expireAfterAccess(expireAfterAccessDuration));
    }

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheFactory<ID, R, RRC, CTX> caffeineCache(Caffeine<Object, Object> caffeine) {

        final AsyncCache<ID, RRC> delegateCache = caffeine.buildAsync();

        return __ -> new Cache<>() {

            @Override
            public Mono<Map<ID, RRC>> getAll(Iterable<ID> ids) {
                return fromFuture(delegateCache.getAll(ids, keys -> of()));
            }

            @Override
            public Mono<Map<ID, RRC>> computeAll(Iterable<ID> ids, FetchFunction<ID, RRC> fetchFunction) {
                return fromFuture(delegateCache.getAll(ids, (keys, executor) -> fetchFunction.apply(keys).toFuture()));
            }

            @Override
            public Mono<?> putAll(Map<ID, RRC> newMap) {
                return toMono((Map<ID, RRC> map) -> map.forEach((id, results) -> delegateCache.put(id, completedFuture(results)))).apply(newMap);
            }

            @Override
            public Mono<?> removeAll(Map<ID, RRC> newMap) {
                return toMono((Map<ID, RRC> map) -> also(delegateCache.asMap(), cache -> map.keySet().forEach(cache::remove))).apply(newMap);
            }
        };
    }

    private static Caffeine<Object, Object> defaultBuilder() {
        return newBuilder();
    }
}