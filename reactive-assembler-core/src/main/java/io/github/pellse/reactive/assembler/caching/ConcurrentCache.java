package io.github.pellse.reactive.assembler.caching;

import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;
import reactor.util.retry.RetrySpec;

import java.time.Duration;
import java.util.List;
import java.util.Map;
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

public interface ConcurrentCache {

    interface Lock {
        boolean tryAcquireLock();

        void releaseLock();
    }

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

            private final AtomicLong atomicLock = new AtomicLong();

            private final Lock readLock = new Lock() {

                @Override
                public boolean tryAcquireLock() {
                    var readCount = atomicLock.get();
//                    System.out.println("readCount = " + readCount);
                    return readCount != -1 && atomicLock.compareAndSet(readCount, readCount + 1);
                }

                @Override
                public void releaseLock() { // this should never be called if tryAcquireLock() returned false
//                    System.out.println("readCount release: " + atomicLock.getAndDecrement());
                }
            };

            private final Lock writeLock = new Lock() {

                @Override
                public boolean tryAcquireLock() {
                    var lockAcquired = atomicLock.compareAndSet(0, -1);
//                    System.out.println("lockAcquired = " + lockAcquired);
                    return lockAcquired;
//                    System.out.println("lockAcquired = false");
//                    return false;
                }

                @Override
                public void releaseLock() { // this should never be called if tryAcquireLock() returned false
//                    System.out.println("Write Lock release: " + atomicLock.getAndSet(0));
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
//                        .doOnError(t -> System.out.println("empty: " +  t))
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
