package io.github.pellse.concurrent;

import io.github.pellse.concurrent.LockManager.Lock;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.function.Function;

import static java.time.Duration.ofSeconds;
import static reactor.core.publisher.Mono.*;

public interface ReentrantExecutor {

    Duration DEFAULT_TIMEOUT = ofSeconds(30);

    default <T> Mono<T> withReadLock(Mono<T> mono) {
        return withReadLock(mono, empty());
    }

    default <T> Mono<T> withReadLock(Mono<T> mono, Mono<T> defaultValue) {
        return withReadLock(mono, DEFAULT_TIMEOUT, defaultValue);
    }

    default <T> Mono<T> withReadLock(Mono<T> mono, Duration timeout, Mono<T> defaultValue) {
        return withReadLock(__ -> mono, timeout, defaultValue);
    }

    default <T> Mono<T> withReadLock(Function<WriteLockExecutor, Mono<T>> monoProvider) {
        return withReadLock(monoProvider, empty());
    }

    default <T> Mono<T> withReadLock(Function<WriteLockExecutor, Mono<T>> monoProvider, Mono<T> defaultValue) {
        return withReadLock(monoProvider, DEFAULT_TIMEOUT, defaultValue);
    }

    default <T> Mono<T> withWriteLock(Mono<T> mono) {
        return withWriteLock(mono, empty());
    }

    default <T> Mono<T> withWriteLock(Mono<T> mono, Mono<T> defaultValue) {
        return withWriteLock(mono, DEFAULT_TIMEOUT, defaultValue);
    }

    <T> Mono<T> withReadLock(Function<WriteLockExecutor, Mono<T>> monoProvider, Duration timeout, Mono<T> defaultValue);

    <T> Mono<T> withWriteLock(Mono<T> mono, Duration timeout, Mono<T> defaultValue);

    @FunctionalInterface
    interface WriteLockExecutor {

        default <U> Mono<U> withWriteLock(Mono<U> mono) {
            return withWriteLock(mono, empty());
        }

        <U> Mono<U> withWriteLock(Mono<U> mono, Mono<U> defaultValue);
    }

    static ReentrantExecutor create() {

        final var lockManager = new LockManager();

        return new ReentrantExecutor() {

            @Override
            public <T> Mono<T> withReadLock(Function<WriteLockExecutor, Mono<T>> monoProvider, Duration timeout, Mono<T> defaultValue) {
                return with(monoProvider, LockManager::acquireReadLock, timeout, defaultValue);
            }

            @Override
            public <T> Mono<T> withWriteLock(Mono<T> mono, Duration timeout, Mono<T> defaultValue) {
                return with(__ -> mono, LockManager::acquireWriteLock, timeout, defaultValue);
            }

            private <T> Mono<T> with(Function<WriteLockExecutor, Mono<T>> monoProvider, Function<LockManager, Mono<Lock>> acquireLockStrategy, Duration timeout, Mono<T> defaultValue) {

                return usingWhen(
                        acquireLockStrategy.apply(lockManager),
                        lock -> monoProvider.apply(new WriteLockExecutor() {
                                    @Override
                                    public <U> Mono<U> withWriteLock(Mono<U> mono, Mono<U> defaultValue) {
                                        return usingWhen(
                                                lockManager.convertToWriteLock(lock),
                                                writeLock -> mono.timeout(timeout, defaultValue),
                                                writeLock -> fromRunnable(writeLock::release)
                                        );
                                    }
                                }).timeout(timeout, defaultValue),
                        lock -> fromRunnable(lock::release)
                );
            }
        };
    }
}