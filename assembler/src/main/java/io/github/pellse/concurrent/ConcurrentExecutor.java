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

package io.github.pellse.concurrent;

import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;
import reactor.util.retry.RetrySpec;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import static io.github.pellse.concurrent.ConcurrentExecutor.ConcurrencyStrategy.WRITE;
import static io.github.pellse.util.ObjectUtils.get;
import static reactor.core.publisher.Mono.*;
import static reactor.util.retry.Retry.*;

@FunctionalInterface
public interface ConcurrentExecutor {

    <T> Mono<T> execute(Mono<T> mono, ConcurrencyStrategy concurrencyStrategy, Mono<T> defaultValueProvider);

    default <T> Mono<T> execute(Mono<T> mono) {
        return execute(mono, empty());
    }

    default <T> Mono<T> execute(Mono<T> mono, Mono<T> defaultValueProvider) {
        return execute(mono, WRITE, defaultValueProvider);
    }

    LockNotAcquiredException LOCK_NOT_ACQUIRED = new LockNotAcquiredException();

    static ConcurrentExecutor concurrentExecutor() {
        return concurrentExecutor(indefinitely());
    }

    static ConcurrentExecutor concurrentExecutor(long maxAttempts) {
        return concurrentExecutor(max(maxAttempts));
    }

    static ConcurrentExecutor concurrentExecutor(long maxAttempts, Duration minBackoff) {
        return concurrentExecutor(backoff(maxAttempts, minBackoff));
    }

    static ConcurrentExecutor concurrentExecutor(RetrySpec retrySpec) {
        return concurrentExecutor(retrySpec, RetrySpec::filter);
    }

    static ConcurrentExecutor concurrentExecutor(RetryBackoffSpec retrySpec) {
        return concurrentExecutor(retrySpec, (Scheduler) null);
    }

    static ConcurrentExecutor concurrentExecutor(RetryBackoffSpec retrySpec, Scheduler retryScheduler) {
        return concurrentExecutor(retrySpec.scheduler(retryScheduler), RetryBackoffSpec::filter);
    }

    private static <T extends Retry> ConcurrentExecutor concurrentExecutor(T retrySpec, BiFunction<T, Predicate<? super Throwable>, T> errorFilterFunction) {
        return concurrentExecutor(errorFilterFunction.apply(retrySpec, LOCK_NOT_ACQUIRED::equals));
    }

    private static ConcurrentExecutor concurrentExecutor(Retry retrySpec) {

        final var isLocked = new AtomicBoolean();
        final var readCount = new AtomicLong();

        class ReadLock implements Lock {

            private volatile boolean lockAcquired = false;

            @Override
            public boolean tryAcquire() {
                if (isLocked.compareAndSet(false, true)) {
                    readCount.incrementAndGet();
                    lockAcquired = true;
                    isLocked.set(false);
                    return true;
                }
                return false;
            }

            @Override
            public boolean release() {
                if (lockAcquired) {
                    readCount.decrementAndGet();
                }
                return true;
            }
        }

        class WriteLock implements Lock {

            @Override
            public boolean tryAcquire() {
                if (isLocked.compareAndSet(false, true)) {
                    if (readCount.get() == 0) {
                        return true;
                    }
                    isLocked.set(false);
                }
                return false;
            }

            @Override
            public boolean release() {
                return isLocked.compareAndSet(true, false);
            }
        }

        final var writeLock = new WriteLock();

        return new ConcurrentExecutor() {

            @Override
            public <T> Mono<T> execute(Mono<T> mono, ConcurrencyStrategy concurrencyStrategy, Mono<T> defaultMono) {
                return usingWhen(
                        just(isLocked),
                        lock -> fromSupplier(() -> lock.compareAndSet(false, true))
                                .filter(isLocked -> isLocked)
                                .switchIfEmpty(error(LOCK_NOT_ACQUIRED))
                                .flatMap(get(mono))
                                .retryWhen(retrySpec),
                        lock -> fromRunnable(() -> lock.set(false))
                )
                        .doOnError(System.out::println)
                        .onErrorResume(Exceptions::isRetryExhausted, e -> defaultMono);
            }

            private Lock getLock(ConcurrencyStrategy concurrencyStrategy) {
                return switch (concurrencyStrategy) {
                    case READ -> new ReadLock();
                    case WRITE -> writeLock;
                };
            }
        };
    }

    enum ConcurrencyStrategy {
        READ,
        WRITE
    }

    interface Lock {
        boolean tryAcquire();

        boolean release();
    }

    class LockNotAcquiredException extends Exception {
        LockNotAcquiredException() {
            super(null, null, true, false);
        }
    }
}
