package io.github.pellse.reactive.assembler.caching;

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
import java.util.function.Predicate;

import static io.github.pellse.util.ObjectUtils.also;
import static io.github.pellse.util.ObjectUtils.run;
import static reactor.core.publisher.Mono.*;
import static reactor.util.retry.Retry.*;

public interface ConcurrentCache {

    class LockNotAcquiredException extends Exception {
    }

    interface Lock {
        boolean tryAcquireLock();

        void releaseLock();
    }

    LockNotAcquiredException LOCK_NOT_ACQUIRED = new LockNotAcquiredException();

    static <ID, R> Cache<ID, R> concurrent(Cache<ID, R> delegateCache) {
        return concurrent(delegateCache, indefinitely(), RetrySpec::filter);
    }

    static <ID, R> Cache<ID, R> concurrent(Cache<ID, R> delegateCache, long maxAttempts) {
        return concurrent(delegateCache, max(maxAttempts), RetrySpec::filter);
    }

    static <ID, R> Cache<ID, R> concurrent(Cache<ID, R> delegateCache, long maxAttempts, Duration delay) {
        return concurrent(delegateCache, fixedDelay(maxAttempts, delay), RetryBackoffSpec::filter);
    }

    private static <ID, R, T extends Retry> Cache<ID, R> concurrent(
            Cache<ID, R> delegateCache,
            T retrySpec,
            BiFunction<T, Predicate<? super Throwable>, T> errorFilterFunction) {

        return concurrent(delegateCache, retryStrategy(retrySpec, errorFilterFunction));
    }

    private static <ID, R> Cache<ID, R> concurrent(Cache<ID, R> delegateCache, Retry retrySpec) {

        return new Cache<>() {

            private final AtomicBoolean isLocked = new AtomicBoolean();

            private final AtomicLong readCount = new AtomicLong();

            private final Lock readLock = new Lock() {

                @Override
                public boolean tryAcquireLock() {
                    if (isLocked.compareAndSet(false, true)) {
                        if (readCount.getAndIncrement() < 0) {
                            throw new IllegalStateException("readCount cannot be < 0 in readLock.tryAcquireLock()");
                        }
                        isLocked.set(false);
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

            private final Lock writeLock = new Lock() {

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
                return execute(delegateCache.getAll(ids, computeIfAbsent), readLock);
            }

            @Override
            public Mono<?> putAll(Map<ID, List<R>> map) {
                return execute(delegateCache.putAll(map), writeLock);
            }

            @Override
            public Mono<?> removeAll(Map<ID, List<R>> map) {
                return execute(delegateCache.removeAll(map), writeLock);
            }

            @Override
            public Mono<?> updateAll(Map<ID, List<R>> mapToAdd, Map<ID, List<R>> mapToRemove) {
                return execute(delegateCache.updateAll(mapToAdd, mapToRemove), writeLock);
            }

            private <T> Mono<T> execute(Mono<T> mono, Lock lock) {

                return defer(() -> {
                    var lockAlreadyReleased = new AtomicBoolean();
                    var lockAcquired = new AtomicBoolean();

                    Runnable releaseLock = () -> {
                        if (lockAcquired.get() && lockAlreadyReleased.compareAndSet(false, true)) {
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
                            .doOnSuccess(run(releaseLock));
                });
            }
        };
    }

    private static <T extends Retry> T retryStrategy(
            T retrySpec,
            BiFunction<T, Predicate<? super Throwable>, T> errorFilterFunction) {

        return errorFilterFunction.apply(retrySpec, LOCK_NOT_ACQUIRED::equals);
    }
}
