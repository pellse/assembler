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
import static io.github.pellse.util.ObjectUtils.*;
import static reactor.core.publisher.Mono.*;
import static reactor.util.retry.Retry.*;

@FunctionalInterface
public interface ConcurrentExecutor<T> {

    LockNotAcquiredException LOCK_NOT_ACQUIRED = new LockNotAcquiredException();

    static <T> ConcurrentExecutor<T> concurrentExecutor() {
        return concurrentExecutor(indefinitely());
    }

    static <T> ConcurrentExecutor<T> concurrentExecutor(long maxAttempts) {
        return concurrentExecutor(max(maxAttempts));
    }

    static <T> ConcurrentExecutor<T> concurrentExecutor(long maxAttempts, Duration minBackoff) {
        return concurrentExecutor(backoff(maxAttempts, minBackoff));
    }

    static <T> ConcurrentExecutor<T> concurrentExecutor(RetrySpec retrySpec) {
        return concurrentExecutor(retrySpec, RetrySpec::filter);
    }

    static <T> ConcurrentExecutor<T> concurrentExecutor(RetryBackoffSpec retrySpec) {
        return concurrentExecutor(retrySpec, (Scheduler) null);
    }

    static <T> ConcurrentExecutor<T> concurrentExecutor(RetryBackoffSpec retrySpec, Scheduler retryScheduler) {
        return concurrentExecutor(retrySpec.scheduler(retryScheduler), RetryBackoffSpec::filter);
    }

    private static <T, R extends Retry> ConcurrentExecutor<T> concurrentExecutor(R retrySpec, BiFunction<R, Predicate<? super Throwable>, R> errorFilterFunction) {
        return concurrentExecutor(errorFilterFunction.apply(retrySpec, LOCK_NOT_ACQUIRED::equals));
    }

    private static <T> ConcurrentExecutor<T> concurrentExecutor(Retry retrySpec) {

        final var isLocked = new AtomicBoolean();
        final var readCount = new AtomicLong();

        final var readLock = new Lock() {

            @Override
            public boolean tryAcquireLock() {
                if (isLocked.compareAndSet(false, true)) {
                    readCount.getAndIncrement();
                    isLocked.set(false);
                    return true;
                }
                return false;
            }

            @Override
            public void releaseLock() {
                readCount.decrementAndGet();
            }
        };

        final var writeLock = new Lock() {

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

        return (mono, concurrencyStrategy) -> {

            final var lock = concurrencyStrategy.equals(WRITE) ? writeLock : readLock;

            return defer(() -> {
                final var lockAcquired = new AtomicBoolean();

                final Runnable releaseLock = () -> {
                    if (lockAcquired.compareAndSet(true, false)) {
                        lock.releaseLock();
                    }
                };

                return fromSupplier(lock::tryAcquireLock)
                        .filter(isLockAcquired -> also(isLockAcquired, lockAcquired::set))
                        .switchIfEmpty(error(LOCK_NOT_ACQUIRED))
                        .retryWhen(retrySpec)
                        .flatMap(get(mono))
                        .doOnError(run(releaseLock))
                        .doOnCancel(releaseLock)
                        .doOnSuccess(run(releaseLock))
                        .onErrorResume(Exceptions::isRetryExhausted, get(Mono::empty));
            });
        };
    }

    default Mono<T> execute(Mono<T> mono) {
        return execute(mono, WRITE);
    }

    Mono<T> execute(Mono<T> mono, ConcurrencyStrategy concurrencyStrategy);

    enum ConcurrencyStrategy {
        READ,
        WRITE
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
