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

package io.github.pellse.reactive.assembler.caching;

import io.github.pellse.reactive.assembler.caching.CacheFactory.CacheTransformer;
import io.github.pellse.reactive.assembler.caching.CacheFactory.FetchFunction;
import io.github.pellse.reactive.assembler.caching.CacheFactory.FetchFunction.NonEmptyFetchFunction;
import io.github.pellse.reactive.assembler.caching.ConcurrentCache.ConcurrencyStrategy;
import reactor.util.retry.RetryBackoffSpec;
import reactor.util.retry.RetrySpec;

import java.time.Duration;

import static io.github.pellse.reactive.assembler.caching.ConcurrentCache.ConcurrencyStrategy.MULTIPLE_READERS;
import static io.github.pellse.reactive.assembler.caching.ConcurrentCache.ConcurrencyStrategy.SINGLE_READER;
import static io.github.pellse.reactive.assembler.caching.ConcurrentCache.concurrentCache;

public interface ConcurrentCacheFactory {

    static <ID, R, RRC> CacheTransformer<ID, R, RRC> concurrent() {
        return ConcurrentCacheFactory::concurrent;
    }

    static <ID, R, RRC> CacheTransformer<ID, R, RRC> concurrent(ConcurrencyStrategy concurrencyStrategy) {
        return cacheFactory -> concurrent(cacheFactory, concurrencyStrategy);
    }

    static <ID, R, RRC> CacheTransformer<ID, R, RRC> concurrent(long maxAttempts) {
        return cacheFactory -> concurrent(cacheFactory, maxAttempts);
    }

    static <ID, R, RRC> CacheTransformer<ID, R, RRC> concurrent(long maxAttempts, ConcurrencyStrategy concurrencyStrategy) {
        return cacheFactory -> concurrent(cacheFactory, maxAttempts, concurrencyStrategy);
    }

    static <ID, R, RRC> CacheTransformer<ID, R, RRC> concurrent(long maxAttempts, Duration minBackoff) {
        return cacheFactory -> concurrent(cacheFactory, maxAttempts, minBackoff);
    }

    static <ID, R, RRC> CacheTransformer<ID, R, RRC> concurrent(long maxAttempts, Duration minBackoff, ConcurrencyStrategy concurrencyStrategy) {
        return cacheFactory -> concurrent(cacheFactory, maxAttempts, minBackoff, concurrencyStrategy);
    }

    static <ID, R, RRC> CacheTransformer<ID, R, RRC> concurrent(RetrySpec retrySpec) {
        return cacheFactory -> concurrent(cacheFactory, retrySpec);
    }

    static <ID, R, RRC> CacheTransformer<ID, R, RRC> concurrent(RetrySpec retrySpec, ConcurrencyStrategy concurrencyStrategy) {
        return cacheFactory -> concurrent(cacheFactory, retrySpec, concurrencyStrategy);
    }

    static <ID, R, RRC> CacheTransformer<ID, R, RRC> concurrent(RetryBackoffSpec retrySpec) {
        return cacheFactory -> concurrent(cacheFactory, retrySpec);
    }

    static <ID, R, RRC> CacheTransformer<ID, R, RRC> concurrent(RetryBackoffSpec retrySpec, ConcurrencyStrategy concurrencyStrategy) {
        return cacheFactory -> concurrent(cacheFactory, retrySpec, concurrencyStrategy);
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> concurrent(CacheFactory<ID, R, RRC> delegateCacheFactory) {
        return (fetchFunction, context) -> concurrentCache(delegateCacheFactory.create(fetchFunction, context), concurrencyStrategy(fetchFunction));
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> concurrent(CacheFactory<ID, R, RRC> delegateCacheFactory, ConcurrencyStrategy concurrencyStrategy) {
        return (fetchFunction, context) -> concurrentCache(delegateCacheFactory.create(fetchFunction, context), concurrencyStrategy);
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> concurrent(CacheFactory<ID, R, RRC> delegateCacheFactory, long maxAttempts) {
        return (fetchFunction, context) -> concurrentCache(delegateCacheFactory.create(fetchFunction, context), maxAttempts, concurrencyStrategy(fetchFunction));
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> concurrent(CacheFactory<ID, R, RRC> delegateCacheFactory, long maxAttempts, ConcurrencyStrategy concurrencyStrategy) {
        return (fetchFunction, context) -> concurrentCache(delegateCacheFactory.create(fetchFunction, context), maxAttempts, concurrencyStrategy);
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> concurrent(CacheFactory<ID, R, RRC> delegateCacheFactory, long maxAttempts, Duration minBackoff) {
        return (fetchFunction, context) -> concurrentCache(delegateCacheFactory.create(fetchFunction, context), maxAttempts, minBackoff, concurrencyStrategy(fetchFunction));
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> concurrent(CacheFactory<ID, R, RRC> delegateCacheFactory, long maxAttempts, Duration minBackoff, ConcurrencyStrategy concurrencyStrategy) {
        return (fetchFunction, context) -> concurrentCache(delegateCacheFactory.create(fetchFunction, context), maxAttempts, minBackoff, concurrencyStrategy);
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> concurrent(CacheFactory<ID, R, RRC> delegateCacheFactory, RetrySpec retrySpec) {
        return (fetchFunction, context) -> concurrentCache(delegateCacheFactory.create(fetchFunction, context), retrySpec, concurrencyStrategy(fetchFunction));
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> concurrent(CacheFactory<ID, R, RRC> delegateCacheFactory, RetrySpec retrySpec, ConcurrencyStrategy concurrencyStrategy) {
        return (fetchFunction, context) -> concurrentCache(delegateCacheFactory.create(fetchFunction, context), retrySpec, concurrencyStrategy);
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> concurrent(CacheFactory<ID, R, RRC> delegateCacheFactory, RetryBackoffSpec retrySpec) {
        return (fetchFunction, context) -> concurrentCache(delegateCacheFactory.create(fetchFunction, context), retrySpec, concurrencyStrategy(fetchFunction));
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> concurrent(CacheFactory<ID, R, RRC> delegateCacheFactory, RetryBackoffSpec retrySpec, ConcurrencyStrategy concurrencyStrategy) {
        return (fetchFunction, context) -> concurrentCache(delegateCacheFactory.create(fetchFunction, context), retrySpec, concurrencyStrategy);
    }

    private static <ID, R> ConcurrencyStrategy concurrencyStrategy(FetchFunction<ID, R> fetchFunction) {
        return fetchFunction instanceof NonEmptyFetchFunction<ID, R> ? SINGLE_READER : MULTIPLE_READERS;
    }
}