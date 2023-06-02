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

import io.github.pellse.reactive.assembler.caching.CacheFactory.CacheContext;
import io.github.pellse.reactive.assembler.caching.CacheFactory.CacheTransformer;
import io.github.pellse.reactive.assembler.caching.ConcurrentCache.ConcurrencyStrategy;
import reactor.core.scheduler.Scheduler;
import reactor.util.retry.RetryBackoffSpec;
import reactor.util.retry.RetrySpec;

import java.time.Duration;

import static io.github.pellse.reactive.assembler.caching.ConcurrentCache.ConcurrencyStrategy.MULTIPLE_READERS;
import static io.github.pellse.reactive.assembler.caching.ConcurrentCache.ConcurrencyStrategy.SINGLE_READER;
import static io.github.pellse.reactive.assembler.caching.ConcurrentCache.concurrentCache;

public interface ConcurrentCacheFactory {

    static <T, ID, R, RRC> CacheTransformer<T, ID, R, RRC> concurrent() {
        return ConcurrentCacheFactory::concurrent;
    }

    static <T, ID, R, RRC> CacheTransformer<T, ID, R, RRC> concurrent(ConcurrencyStrategy concurrencyStrategy) {
        return cacheFactory -> concurrent(cacheFactory, concurrencyStrategy);
    }

    static <T, ID, R, RRC> CacheTransformer<T, ID, R, RRC> concurrent(long maxAttempts) {
        return cacheFactory -> concurrent(cacheFactory, maxAttempts);
    }

    static <T, ID, R, RRC> CacheTransformer<T, ID, R, RRC> concurrent(long maxAttempts, ConcurrencyStrategy concurrencyStrategy) {
        return cacheFactory -> concurrent(cacheFactory, maxAttempts, concurrencyStrategy);
    }

    static <T, ID, R, RRC> CacheTransformer<T, ID, R, RRC> concurrent(long maxAttempts, Duration minBackoff) {
        return cacheFactory -> concurrent(cacheFactory, maxAttempts, minBackoff);
    }

    static <T, ID, R, RRC> CacheTransformer<T, ID, R, RRC> concurrent(long maxAttempts, Duration minBackoff, ConcurrencyStrategy concurrencyStrategy) {
        return cacheFactory -> concurrent(cacheFactory, maxAttempts, minBackoff, concurrencyStrategy);
    }

    static <T, ID, R, RRC> CacheTransformer<T, ID, R, RRC> concurrent(RetrySpec retrySpec) {
        return cacheFactory -> concurrent(cacheFactory, retrySpec);
    }

    static <T, ID, R, RRC> CacheTransformer<T, ID, R, RRC> concurrent(RetrySpec retrySpec, ConcurrencyStrategy concurrencyStrategy) {
        return cacheFactory -> concurrent(cacheFactory, retrySpec, concurrencyStrategy);
    }

    static <T, ID, R, RRC> CacheTransformer<T, ID, R, RRC> concurrent(RetryBackoffSpec retrySpec) {
        return cacheFactory -> concurrent(cacheFactory, retrySpec);
    }

    static <T, ID, R, RRC> CacheTransformer<T, ID, R, RRC> concurrent(RetryBackoffSpec retrySpec, Scheduler retryScheduler) {
        return cacheFactory -> concurrent(cacheFactory, retrySpec, retryScheduler);
    }

    static <T, ID, R, RRC> CacheTransformer<T, ID, R, RRC> concurrent(RetryBackoffSpec retrySpec, ConcurrencyStrategy concurrencyStrategy) {
        return cacheFactory -> concurrent(cacheFactory, retrySpec, concurrencyStrategy);
    }

    static <T, ID, R, RRC> CacheTransformer<T, ID, R, RRC> concurrent(RetryBackoffSpec retrySpec, ConcurrencyStrategy concurrencyStrategy, Scheduler retryScheduler) {
        return cacheFactory -> concurrent(cacheFactory, retrySpec, concurrencyStrategy, retryScheduler);
    }

    static <T, ID, R, RRC> CacheFactory<T, ID, R, RRC> concurrent(CacheFactory<T, ID, R, RRC> delegateCacheFactory) {
        return context -> concurrentCache(delegateCacheFactory.create(context), concurrencyStrategy(context));
    }

    static <T, ID, R, RRC> CacheFactory<T, ID, R, RRC> concurrent(CacheFactory<T, ID, R, RRC> delegateCacheFactory, ConcurrencyStrategy concurrencyStrategy) {
        return context -> concurrentCache(delegateCacheFactory.create(context), concurrencyStrategy);
    }

    static <T, ID, R, RRC> CacheFactory<T, ID, R, RRC> concurrent(CacheFactory<T, ID, R, RRC> delegateCacheFactory, long maxAttempts) {
        return context -> concurrentCache(delegateCacheFactory.create(context), maxAttempts, concurrencyStrategy(context));
    }

    static <T, ID, R, RRC> CacheFactory<T, ID, R, RRC> concurrent(CacheFactory<T, ID, R, RRC> delegateCacheFactory, long maxAttempts, ConcurrencyStrategy concurrencyStrategy) {
        return context -> concurrentCache(delegateCacheFactory.create(context), maxAttempts, concurrencyStrategy);
    }

    static <T, ID, R, RRC> CacheFactory<T, ID, R, RRC> concurrent(CacheFactory<T, ID, R, RRC> delegateCacheFactory, long maxAttempts, Duration minBackoff) {
        return context -> concurrentCache(delegateCacheFactory.create(context), maxAttempts, minBackoff, concurrencyStrategy(context));
    }

    static <T, ID, R, RRC> CacheFactory<T, ID, R, RRC> concurrent(CacheFactory<T, ID, R, RRC> delegateCacheFactory, long maxAttempts, Duration minBackoff, ConcurrencyStrategy concurrencyStrategy) {
        return context -> concurrentCache(delegateCacheFactory.create(context), maxAttempts, minBackoff, concurrencyStrategy);
    }

    static <T, ID, R, RRC> CacheFactory<T, ID, R, RRC> concurrent(CacheFactory<T, ID, R, RRC> delegateCacheFactory, RetrySpec retrySpec) {
        return context -> concurrentCache(delegateCacheFactory.create(context), retrySpec, concurrencyStrategy(context));
    }

    static <T, ID, R, RRC> CacheFactory<T, ID, R, RRC> concurrent(CacheFactory<T, ID, R, RRC> delegateCacheFactory, RetrySpec retrySpec, ConcurrencyStrategy concurrencyStrategy) {
        return context -> concurrentCache(delegateCacheFactory.create(context), retrySpec, concurrencyStrategy);
    }

    static <T, ID, R, RRC> CacheFactory<T, ID, R, RRC> concurrent(CacheFactory<T, ID, R, RRC> delegateCacheFactory, RetryBackoffSpec retrySpec) {
        return context -> concurrentCache(delegateCacheFactory.create(context), retrySpec, concurrencyStrategy(context));
    }

    static <T, ID, R, RRC> CacheFactory<T, ID, R, RRC> concurrent(CacheFactory<T, ID, R, RRC> delegateCacheFactory, RetryBackoffSpec retrySpec, Scheduler retryScheduler) {
        return context -> concurrentCache(delegateCacheFactory.create(context), retrySpec, concurrencyStrategy(context), retryScheduler);
    }

    static <T, ID, R, RRC> CacheFactory<T, ID, R, RRC> concurrent(CacheFactory<T, ID, R, RRC> delegateCacheFactory, RetryBackoffSpec retrySpec, ConcurrencyStrategy concurrencyStrategy) {
        return context -> concurrentCache(delegateCacheFactory.create(context), retrySpec, concurrencyStrategy);
    }

    static <T, ID, R, RRC> CacheFactory<T, ID, R, RRC> concurrent(CacheFactory<T, ID, R, RRC> delegateCacheFactory, RetryBackoffSpec retrySpec, ConcurrencyStrategy concurrencyStrategy, Scheduler retryScheduler) {
        return context -> concurrentCache(delegateCacheFactory.create(context), retrySpec, concurrencyStrategy, retryScheduler);
    }

    private static <ID, R, RRC> ConcurrencyStrategy concurrencyStrategy(CacheContext<ID, R, RRC> context) {
        return context.isEmptySource() ? MULTIPLE_READERS : SINGLE_READER ;
    }
}