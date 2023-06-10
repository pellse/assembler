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

package io.github.pellse.cohereflux.caching;

import io.github.pellse.concurrent.ConcurrentExecutor;
import io.github.pellse.concurrent.ConcurrentExecutor.ConcurrencyStrategy;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.util.retry.RetryBackoffSpec;
import reactor.util.retry.RetrySpec;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static io.github.pellse.concurrent.ConcurrentExecutor.ConcurrencyStrategy.WRITE;
import static io.github.pellse.concurrent.ConcurrentExecutor.concurrentExecutor;
import static reactor.util.retry.Retry.indefinitely;

public interface ConcurrentCache<ID, R> extends Cache<ID, R> {

    static <ID, R> ConcurrentCache<ID, R> concurrentCache(Cache<ID, R> delegateCache) {
        return concurrentCache(delegateCache, WRITE);
    }

    static <ID, R> ConcurrentCache<ID, R> concurrentCache(Cache<ID, R> delegateCache, ConcurrencyStrategy concurrencyStrategy) {
        return concurrentCache(delegateCache, indefinitely(), concurrencyStrategy);
    }

    static <ID, R> ConcurrentCache<ID, R> concurrentCache(Cache<ID, R> delegateCache, long maxAttempts) {
        return concurrentCache(delegateCache, maxAttempts, WRITE);
    }

    static <ID, R> ConcurrentCache<ID, R> concurrentCache(Cache<ID, R> delegateCache, long maxAttempts, ConcurrencyStrategy concurrencyStrategy) {
        return concurrentCache(delegateCache, () -> concurrentExecutor(maxAttempts), concurrencyStrategy);
    }

    static <ID, R> ConcurrentCache<ID, R> concurrentCache(Cache<ID, R> delegateCache, long maxAttempts, Duration minBackoff) {
        return concurrentCache(delegateCache, maxAttempts, minBackoff, WRITE);
    }

    static <ID, R> ConcurrentCache<ID, R> concurrentCache(Cache<ID, R> delegateCache, long maxAttempts, Duration minBackoff, ConcurrencyStrategy concurrencyStrategy) {
        return concurrentCache(delegateCache, () -> concurrentExecutor(maxAttempts, minBackoff), concurrencyStrategy);
    }

    static <ID, R> ConcurrentCache<ID, R> concurrentCache(Cache<ID, R> delegateCache, RetrySpec retrySpec) {
        return concurrentCache(delegateCache, retrySpec, WRITE);
    }

    static <ID, R> ConcurrentCache<ID, R> concurrentCache(Cache<ID, R> delegateCache, RetrySpec retrySpec, ConcurrencyStrategy concurrencyStrategy) {
        return concurrentCache(delegateCache, () -> concurrentExecutor(retrySpec), concurrencyStrategy);
    }

    static <ID, R> ConcurrentCache<ID, R> concurrentCache(Cache<ID, R> delegateCache, RetryBackoffSpec retrySpec) {
        return concurrentCache(delegateCache, retrySpec, WRITE);
    }

    static <ID, R> ConcurrentCache<ID, R> concurrentCache(Cache<ID, R> delegateCache, RetryBackoffSpec retrySpec, ConcurrencyStrategy concurrencyStrategy) {
        return concurrentCache(delegateCache, () -> concurrentExecutor(retrySpec), concurrencyStrategy);
    }

    static <ID, R> ConcurrentCache<ID, R> concurrentCache(Cache<ID, R> delegateCache, RetryBackoffSpec retrySpec, ConcurrencyStrategy concurrencyStrategy, Scheduler retryScheduler) {
        return concurrentCache(delegateCache, () -> concurrentExecutor(retrySpec, retryScheduler), concurrencyStrategy);
    }

    private static <ID, R> ConcurrentCache<ID, R> concurrentCache(Cache<ID, R> delegateCache, Supplier<ConcurrentExecutor> executorSupplier, ConcurrencyStrategy concurrencyStrategy) {

        if (delegateCache instanceof ConcurrentCache<ID, R> concurrentCache) {
            return concurrentCache;
        }

        final var executor = executorSupplier.get();

        return new ConcurrentCache<>() {

            @Override
            public Mono<Map<ID, List<R>>> getAll(Iterable<ID> ids, FetchFunction<ID, R> fetchFunction) {
                return executor.execute(delegateCache.getAll(ids, fetchFunction), concurrencyStrategy);
            }

            @Override
            public Mono<?> putAll(Map<ID, List<R>> map) {
                return executor.execute(delegateCache.putAll(map));
            }

            @Override
            public Mono<?> removeAll(Map<ID, List<R>> map) {
                return executor.execute(delegateCache.removeAll(map));
            }

            @Override
            public Mono<?> updateAll(Map<ID, List<R>> mapToAdd, Map<ID, List<R>> mapToRemove) {
                return executor.execute(delegateCache.updateAll(mapToAdd, mapToRemove));
            }
        };
    }
}
