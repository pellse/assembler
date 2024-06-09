package io.github.pellse.concurrent;

import io.github.pellse.concurrent.LockManager.Lock;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.github.pellse.util.reactive.ReactiveUtils.nullToEmpty;
import static java.time.Duration.ofSeconds;
import static reactor.core.publisher.Mono.*;

public interface ReentrantExecutor {

    @FunctionalInterface
    interface WriteLockExecutor<U> {
        Mono<U> withWriteLock(Mono<U> mono);
    }

    Duration DEFAULT_TIMEOUT = ofSeconds(30);

    default <T> Mono<T> withReadLock(Mono<T> mono) {
        return withReadLock(mono, null);
    }

    default <T> Mono<T> withReadLock(Mono<T> mono, Supplier<T> defaultValueProvider) {
        return withReadLock(mono, DEFAULT_TIMEOUT, defaultValueProvider);
    }

    default <T> Mono<T> withReadLock(Mono<T> mono, Duration timeout, Supplier<T> defaultValueProvider) {
        return withReadLock(__ -> mono, timeout, defaultValueProvider);
    }

    default <T> Mono<T> withReadLock(Function<WriteLockExecutor<T>, Mono<T>> monoProvider) {
        return withReadLock(monoProvider, null);
    }

    default <T> Mono<T> withReadLock(Function<WriteLockExecutor<T>, Mono<T>> monoProvider, Supplier<T> defaultValueProvider) {
        return withReadLock(monoProvider, DEFAULT_TIMEOUT, defaultValueProvider);
    }

    default <T> Mono<T> withWriteLock(Mono<T> mono) {
        return withWriteLock(mono, null);
    }

    default <T> Mono<T> withWriteLock(Mono<T> mono, Supplier<T> defaultValueProvider) {
        return withWriteLock(mono, DEFAULT_TIMEOUT, defaultValueProvider);
    }

    <T> Mono<T> withReadLock(Function<WriteLockExecutor<T>, Mono<T>> monoProvider, Duration timeout, Supplier<T> defaultValueProvider);

    <T> Mono<T> withWriteLock(Mono<T> mono, Duration timeout, Supplier<T> defaultValueProvider);

    static ReentrantExecutor create() {

        final var lockManager = new LockManager();

        return new ReentrantExecutor() {

            @Override
            public <T> Mono<T> withReadLock(Function<WriteLockExecutor<T>, Mono<T>> monoProvider, Duration timeout, Supplier<T> defaultValueProvider) {
                return with(monoProvider, LockManager::acquireReadLock, timeout, defaultValueProvider);
            }

            @Override
            public <T> Mono<T> withWriteLock(Mono<T> mono, Duration timeout, Supplier<T> defaultValueProvider) {
                return with(__ -> mono, LockManager::acquireWriteLock, timeout, defaultValueProvider);
            }

            private <T> Mono<T> with(
                    Function<WriteLockExecutor<T>, Mono<T>> monoProvider,
                    Function<LockManager, Mono<Lock>> acquireLockStrategy,
                    Duration timeout,
                    Supplier<T> defaultValueProvider) {

                final Mono<T> defaultMono = nullToEmpty(defaultValueProvider);

                return usingWhen(
                        acquireLockStrategy.apply(lockManager),
                        lock -> monoProvider.apply(mono -> usingWhen(
                                lockManager.toWriteLock(lock),
                                writeLock -> mono.timeout(timeout, defaultMono),
                                writeLock -> fromRunnable(writeLock::release)
                        )).timeout(timeout, defaultMono),
                        lock -> fromRunnable(lock::release)
                );
            }
        };
    }
}