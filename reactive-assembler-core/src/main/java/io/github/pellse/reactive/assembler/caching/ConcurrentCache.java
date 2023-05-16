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

import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;
import reactor.util.retry.RetrySpec;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.github.pellse.reactive.assembler.caching.ConcurrentCache.ConcurrencyStrategy.SINGLE_READER;
import static io.github.pellse.util.ObjectUtils.also;
import static io.github.pellse.util.ObjectUtils.run;
import static reactor.core.publisher.Mono.*;
import static reactor.util.retry.Retry.*;

public interface ConcurrentCache<ID, R> extends Cache<ID, R> {

    LockNotAcquiredException LOCK_NOT_ACQUIRED = new LockNotAcquiredException();

    static <ID, R> ConcurrentCache<ID, R> concurrentCache(Cache<ID, R> delegateCache) {
        return concurrentCache(delegateCache, SINGLE_READER);
    }

    static <ID, R> ConcurrentCache<ID, R> concurrentCache(Cache<ID, R> delegateCache, ConcurrencyStrategy concurrencyStrategy) {
        return concurrentCache(delegateCache, indefinitely(), concurrencyStrategy);
    }

    static <ID, R> ConcurrentCache<ID, R> concurrentCache(Cache<ID, R> delegateCache, long maxAttempts) {
        return concurrentCache(delegateCache, maxAttempts, SINGLE_READER);
    }

    static <ID, R> ConcurrentCache<ID, R> concurrentCache(Cache<ID, R> delegateCache, long maxAttempts, ConcurrencyStrategy concurrencyStrategy) {
        return concurrentCache(delegateCache, max(maxAttempts), RetrySpec::filter, concurrencyStrategy);
    }

    static <ID, R> ConcurrentCache<ID, R> concurrentCache(Cache<ID, R> delegateCache, long maxAttempts, Duration minBackoff) {
        return concurrentCache(delegateCache, maxAttempts, minBackoff, SINGLE_READER);
    }

    static <ID, R> ConcurrentCache<ID, R> concurrentCache(Cache<ID, R> delegateCache, long maxAttempts, Duration minBackoff, ConcurrencyStrategy concurrencyStrategy) {
        return concurrentCache(delegateCache, backoff(maxAttempts, minBackoff).jitter(0.75), RetryBackoffSpec::filter, concurrencyStrategy);
    }

    static <ID, R> ConcurrentCache<ID, R> concurrentCache(Cache<ID, R> delegateCache, RetrySpec retrySpec) {
        return concurrentCache(delegateCache, retrySpec, SINGLE_READER);
    }

    static <ID, R> ConcurrentCache<ID, R> concurrentCache(Cache<ID, R> delegateCache, RetrySpec retrySpec, ConcurrencyStrategy concurrencyStrategy) {
        return concurrentCache(delegateCache, retrySpec, RetrySpec::filter, concurrencyStrategy);
    }

    static <ID, R> ConcurrentCache<ID, R> concurrentCache(Cache<ID, R> delegateCache, RetryBackoffSpec retrySpec) {
        return concurrentCache(delegateCache, retrySpec, SINGLE_READER);
    }

    static <ID, R> ConcurrentCache<ID, R> concurrentCache(Cache<ID, R> delegateCache, RetryBackoffSpec retrySpec, ConcurrencyStrategy concurrencyStrategy) {
        return concurrentCache(delegateCache, retrySpec, RetryBackoffSpec::filter, concurrencyStrategy);
    }

    private static <ID, R, T extends Retry> ConcurrentCache<ID, R> concurrentCache(
            Cache<ID, R> delegateCache,
            T retrySpec,
            BiFunction<T, Predicate<? super Throwable>, T> errorFilterFunction,
            ConcurrencyStrategy concurrencyStrategy) {

        return build(delegateCache, cache -> concurrentCache(cache, retryStrategy(retrySpec, errorFilterFunction), concurrencyStrategy));
    }

    private static <ID, R> ConcurrentCache<ID, R> concurrentCache(Cache<ID, R> delegateCache, Retry retrySpec, ConcurrencyStrategy concurrencyStrategy) {

        return new ConcurrentCache<>() {

            private final AtomicBoolean isLocked = new AtomicBoolean();

            private final AtomicLong readCount = new AtomicLong();

            private final Lock multipleReadersLock = new Lock() {

                @Override
                public boolean tryAcquireLock() {
                    if (isLocked.compareAndSet(false, true)) {
                        try {
                            if (readCount.getAndIncrement() < 0) {
                                throw new IllegalStateException("readCount cannot be < 0 in readLock.tryAcquireLock()");
                            }
                        } finally {
                            isLocked.set(false);
                        }
                        return true;
                    }
                    return false;
                }

                @Override
                public void releaseLock() {
                    if (readCount.decrementAndGet() < 0) {
                        throw new IllegalStateException("readCount cannot be < 0 in readLock.releaseLock()");
                    }
                }
            };

            private final Lock singleReaderLock = new Lock() {

                @Override
                public boolean tryAcquireLock() {
                    if (isLocked.compareAndSet(false, true)) {
                        if (readCount.get() == 0) {
                            return true;
                        }
                        isLocked.set(false);
                    }
                    return false;
                }

                @Override
                public void releaseLock() {
                    isLocked.set(false);
                }
            };

            @Override
            public Mono<Map<ID, List<R>>> getAll(Iterable<ID> ids, boolean computeIfAbsent) {
                return execute(delegateCache.getAll(ids, computeIfAbsent), concurrencyStrategy.equals(SINGLE_READER) ? singleReaderLock : multipleReadersLock);
            }

            @Override
            public Mono<?> putAll(Map<ID, List<R>> map) {
                return execute(delegateCache.putAll(map), singleReaderLock);
            }

            @Override
            public Mono<?> removeAll(Map<ID, List<R>> map) {
                return execute(delegateCache.removeAll(map), singleReaderLock);
            }

            @Override
            public Mono<?> updateAll(Map<ID, List<R>> mapToAdd, Map<ID, List<R>> mapToRemove) {
                return execute(delegateCache.updateAll(mapToAdd, mapToRemove), singleReaderLock);
            }

            private <T> Mono<T> execute(Mono<T> mono, Lock lock) {

                return defer(() -> {
                    final var lockAcquired = new AtomicBoolean();

                    final Runnable releaseLock = () -> {
                        if (lockAcquired.compareAndSet(true, false)) {
                            lock.releaseLock();
                        }
                    };

                    return fromSupplier(lock::tryAcquireLock)
                            .filter(isLocked -> also(isLocked, lockAcquired::set))
                            .switchIfEmpty(error(LOCK_NOT_ACQUIRED))
                            .retryWhen(retrySpec)
                            .flatMap(__ -> mono)
                            .doOnError(run(releaseLock))
                            .doOnCancel(releaseLock)
                            .doOnSuccess(run(releaseLock))
                            .onErrorResume(Exceptions::isRetryExhausted, e -> empty());
                });
            }
        };
    }

    private static <T extends Retry> T retryStrategy(
            T retrySpec,
            BiFunction<T, Predicate<? super Throwable>, T> errorFilterFunction) {

        return errorFilterFunction.apply(retrySpec, LOCK_NOT_ACQUIRED::equals);
    }

    private static <ID, R> ConcurrentCache<ID, R> build(Cache<ID, R> delegateCache, Function<Cache<ID, R>, ConcurrentCache<ID, R>> f) {
        return delegateCache instanceof ConcurrentCache<ID, R> c ? c : f.apply(delegateCache);
    }

    enum ConcurrencyStrategy {
        SINGLE_READER,
        MULTIPLE_READERS
    }

    interface Lock {
        boolean tryAcquireLock();

        void releaseLock();
    }

    class LockNotAcquiredException extends Exception {
        LockNotAcquiredException() {
            super(null, null, true, false);
        }
    }
}
