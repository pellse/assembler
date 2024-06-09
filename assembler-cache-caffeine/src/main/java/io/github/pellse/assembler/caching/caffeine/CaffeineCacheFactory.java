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
import io.github.pellse.assembler.caching.CacheContext;
import io.github.pellse.assembler.caching.CacheFactory;

import java.time.Duration;
import java.util.function.Function;

import static com.github.benmanes.caffeine.cache.Caffeine.newBuilder;
import static io.github.pellse.assembler.caching.Cache.adapterCache;
import static io.github.pellse.assembler.caching.CacheFactory.toMono;
import static io.github.pellse.util.ObjectUtils.also;
import static java.util.Map.of;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static reactor.core.publisher.Mono.fromFuture;

public interface CaffeineCacheFactory {

    static <ID, EID, R, RRC, CTX extends CacheContext<ID, EID, R, RRC>> CacheFactory<ID, EID, R, RRC, CTX> caffeineCache() {
        return caffeineCache(newBuilder());
    }

    static <ID, EID, R, RRC, CTX extends CacheContext<ID, EID, R, RRC>> CacheFactory<ID, EID, R, RRC, CTX> caffeineCache(long maxSize) {
        return caffeineCache(newBuilder().maximumSize(maxSize));
    }

    static <ID, EID, R, RRC, CTX extends CacheContext<ID, EID, R, RRC>> CacheFactory<ID, EID, R, RRC, CTX> caffeineCache(Duration expireAfterAccessDuration) {
        return caffeineCache(newBuilder().expireAfterAccess(expireAfterAccessDuration));
    }

    static <ID, EID, R, RRC, CTX extends CacheContext<ID, EID, R, RRC>> CacheFactory<ID, EID, R, RRC, CTX> caffeineCache(long maxSize, Duration expireAfterAccessDuration) {
        return caffeineCache(newBuilder()
                .maximumSize(maxSize)
                .expireAfterAccess(expireAfterAccessDuration));
    }

    static <ID, EID, R, RRC, CTX extends CacheContext<ID, EID, R, RRC>> CacheFactory<ID, EID, R, RRC, CTX> caffeineCache(Function<Caffeine<Object, Object>, Caffeine<Object, Object>> customizer) {
        return caffeineCache(customizer.apply(newBuilder()));
    }

    static <ID, EID, R, RRC, CTX extends CacheContext<ID, EID, R, RRC>> CacheFactory<ID, EID, R, RRC, CTX> caffeineCache(Caffeine<Object, Object> caffeine) {

        final AsyncCache<ID, RRC> delegateCache = caffeine.buildAsync();

        return __ -> adapterCache(
                ids -> fromFuture(delegateCache.getAll(ids, keys -> of())),
                (ids, fetchFunction) -> fromFuture(delegateCache.getAll(ids, (keys, executor) -> fetchFunction.apply(keys).toFuture())),
                toMono(map -> map.forEach((id, results) -> delegateCache.put(id, completedFuture(results)))),
                toMono(map -> also(delegateCache.asMap(), cache -> map.keySet().forEach(cache::remove)))
        );
    }
}