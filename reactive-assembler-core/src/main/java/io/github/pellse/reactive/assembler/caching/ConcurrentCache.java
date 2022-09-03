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

import static io.github.pellse.util.ObjectUtils.run;
import static io.github.pellse.util.ObjectUtils.runIf;
import static java.util.function.Predicate.not;
import static reactor.core.publisher.Mono.*;
import static reactor.util.retry.Retry.*;

class LockNotAcquiredException extends Exception {
}

interface Lock {
    boolean tryAcquireLock();

    void releaseLock();
}

public interface ConcurrentCache {

    LockNotAcquiredException LOCK_NOT_ACQUIRED = new LockNotAcquiredException();

    static <ID, R> Cache<ID, R> concurrent(Cache<ID, R> delegateCache) {
        return concurrent(delegateCache, retryStrategy(indefinitely(), RetrySpec::filter));
    }

    static <ID, R> Cache<ID, R> concurrent(Cache<ID, R> delegateCache, long maxAttempts) {
        return concurrent(delegateCache, retryStrategy(max(maxAttempts), RetrySpec::filter));
    }

    static <ID, R> Cache<ID, R> concurrent(Cache<ID, R> delegateCache, long maxAttempts, Duration delay) {
        return concurrent(delegateCache, retryStrategy(fixedDelay(maxAttempts, delay), RetryBackoffSpec::filter));
    }

    static <ID, R> Cache<ID, R> concurrent(Cache<ID, R> delegateCache, Retry retrySpec) {

        return new Cache<>() {

            private final AtomicBoolean isLocked = new AtomicBoolean();

            private final AtomicLong readCount = new AtomicLong();

            private final Lock readLock = new Lock() {

                @Override
                public boolean tryAcquireLock() {
                    if (isLocked.compareAndSet(false, true)) {
                        readCount.incrementAndGet();
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

                return defer(() -> just(lock.tryAcquireLock()))
                        .filter(lockAcquired -> lockAcquired)
                        .flatMap(__ -> mono)
                        .switchIfEmpty(error(LOCK_NOT_ACQUIRED))
                        .doOnError(runIf(not(LOCK_NOT_ACQUIRED::equals), lock::releaseLock))
                        .doOnSuccess(run(lock::releaseLock))
                        .retryWhen(retrySpec);
            }
        };
    }

    private static <T extends Retry> T retryStrategy(T retrySpec, BiFunction<T, Predicate<? super Throwable>, T> filterFunction) {
        return filterFunction.apply(retrySpec, LOCK_NOT_ACQUIRED::equals);
    }
}
