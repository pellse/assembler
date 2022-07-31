package io.github.pellse.reactive.assembler.caching;

import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;
import reactor.util.retry.RetrySpec;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

import static io.github.pellse.util.ObjectUtils.run;
import static io.github.pellse.util.ObjectUtils.runIf;
import static java.util.function.Predicate.not;
import static reactor.core.publisher.Mono.*;
import static reactor.util.retry.Retry.*;

class LockNotAcquiredException extends Exception {
}

public interface ConcurrentCache {

    LockNotAcquiredException LOCK_NOT_ACQUIRED = new LockNotAcquiredException();

    static <ID, RRC> Cache<ID, RRC> concurrent(Cache<ID, RRC> delegateCache) {
        return concurrent(delegateCache, retryStrategy(indefinitely(), RetrySpec::filter));
    }

    static <ID, RRC> Cache<ID, RRC> concurrent(Cache<ID, RRC> delegateCache, long maxAttempts) {
        return concurrent(delegateCache, retryStrategy(max(maxAttempts), RetrySpec::filter));
    }

    static <ID, RRC> Cache<ID, RRC> concurrent(Cache<ID, RRC> delegateCache, long maxAttempts, Duration delay) {
        return concurrent(delegateCache, retryStrategy(fixedDelay(maxAttempts, delay), RetryBackoffSpec::filter));
    }

    static <ID, RRC> Cache<ID, RRC> concurrent(Cache<ID, RRC> delegateCache, Retry retrySpec) {

        return new Cache<>() {

            private final AtomicBoolean atomicLock = new AtomicBoolean();

            @Override
            public Mono<Map<ID, RRC>> getAll(Iterable<ID> ids, boolean computeIfAbsent) {
                return execute(delegateCache.getAll(ids, computeIfAbsent), this::tryAcquireReadLock);
            }

            @Override
            public Mono<?> putAll(Map<ID, RRC> map) {
                return execute(delegateCache.putAll(map), this::tryAcquireWriteLock);
            }

            @Override
            public Mono<?> removeAll(Map<ID, RRC> map) {
                return execute(delegateCache.removeAll(map), this::tryAcquireWriteLock);
            }

            @Override
            public Mono<?> updateAll(Map<ID, RRC> mapToAdd, Map<ID, RRC> mapToRemove) {
                return execute(delegateCache.updateAll(mapToAdd, mapToRemove), this::tryAcquireWriteLock);
            }

            private <T> Mono<T> execute(Mono<T> mono, BooleanSupplier tryAcquireLock) {

                Runnable releaseLock = () -> atomicLock.set(false);

                return defer(() -> just(tryAcquireLock.getAsBoolean()))
                        .filter(lockAcquired -> lockAcquired)
                        .flatMap(__ -> mono)
                        .switchIfEmpty(error(LOCK_NOT_ACQUIRED))
                        .doOnError(runIf(not(LOCK_NOT_ACQUIRED::equals), releaseLock))
                        .doOnSuccess(run(releaseLock))
                        .retryWhen(retrySpec);
            }

            private boolean tryAcquireReadLock() {
                return !atomicLock.get();
            }

            private boolean tryAcquireWriteLock() {
                return atomicLock.compareAndSet(false, true);
            }
        };
    }

    private static <T extends Retry> T retryStrategy(T retrySpec, BiFunction<T, Predicate<? super Throwable>, T> filterFunction) {
        return filterFunction.apply(retrySpec, LOCK_NOT_ACQUIRED::equals);
    }
}
