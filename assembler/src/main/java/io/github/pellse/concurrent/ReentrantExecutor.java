package io.github.pellse.concurrent;

import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.github.pellse.util.reactive.ReactiveUtils.nullToEmpty;
import static java.time.Duration.ofSeconds;
import static reactor.core.publisher.Mono.*;

public interface ReentrantExecutor {

    Duration DEFAULT_TIMEOUT = ofSeconds(10);

    @FunctionalInterface
    interface WriteLockExecutor<U> {
        Mono<U> withLock(Mono<U> mono);

        default Mono<U> withLock(Supplier<Mono<U>> monoSupplier) {
            return withLock(defer(monoSupplier));
        }
    }

    <T> Mono<T> withReadLock(Function<WriteLockExecutor<T>, Mono<T>> writeLockMonoFunction, Duration timeout, Supplier<T> defaultValueProvider);

    <T> Mono<T> withWriteLock(Mono<T> mono, Duration timeout, Supplier<T> defaultValueProvider);

    default <T> Mono<T> withReadLock(Mono<T> mono) {
        return withReadLock(mono, null);
    }

    default <T> Mono<T> withReadLock(Mono<T> mono, Supplier<T> defaultValueProvider) {
        return withReadLock(mono, DEFAULT_TIMEOUT, defaultValueProvider);
    }

    default <T> Mono<T> withReadLock(Mono<T> mono, Duration timeout, Supplier<T> defaultValueProvider) {
        return withReadLock(__ -> mono, timeout, defaultValueProvider);
    }

    default <T> Mono<T> withReadLock(Function<WriteLockExecutor<T>, Mono<T>> writeLockMonoFunction) {
        return withReadLock(writeLockMonoFunction, null);
    }

    default <T> Mono<T> withReadLock(Function<WriteLockExecutor<T>, Mono<T>> writeLockMonoFunction, Supplier<T> defaultValueProvider) {
        return withReadLock(writeLockMonoFunction, DEFAULT_TIMEOUT, defaultValueProvider);
    }

    default <T> Mono<T> withWriteLock(Mono<T> mono) {
        return withWriteLock(mono, null);
    }

    default <T> Mono<T> withWriteLock(Mono<T> mono, Supplier<T> defaultValueProvider) {
        return withWriteLock(mono, DEFAULT_TIMEOUT, defaultValueProvider);
    }

    static ReentrantExecutor create() {
        final var lockManager = new LockManager();

        return new ReentrantExecutor() {

            @FunctionalInterface
            interface ResourceManager<T> {
                Mono<T> using(Mono<Lock> lockProvider, Function<Lock, Mono<T>> monoProvider);

                default Mono<T> using(Mono<Lock> lockProvider, Mono<T> mono) {
                    return using(lockProvider, __ -> mono);
                }
            }

            @Override
            public <T> Mono<T> withReadLock(Function<WriteLockExecutor<T>, Mono<T>> writeLockMonoFunction, Duration timeout, Supplier<T> defaultValueProvider) {
                return with(writeLockMonoFunction, LockManager::acquireReadLock, timeout, defaultValueProvider);
            }

            @Override
            public <T> Mono<T> withWriteLock(Mono<T> mono, Duration timeout, Supplier<T> defaultValueProvider) {
                return with(mono, LockManager::acquireWriteLock, timeout, defaultValueProvider);
            }

            private <T> Mono<T> with(
                    Mono<T> mono,
                    Function<LockManager, Mono<Lock>> lockAcquisitionStrategy,
                    Duration timeout,
                    Supplier<T> defaultValueProvider) {

                return usingWhen(
                        lockAcquisitionStrategy.apply(lockManager),
                        lock -> mono.timeout(timeout, nullToEmpty(defaultValueProvider)),
                        Lock::release);
            }

            private <T> Mono<T> with(
                    Function<WriteLockExecutor<T>, Mono<T>> writeLockMonoFunction,
                    Function<LockManager, Mono<Lock>> lockAcquisitionStrategy,
                    Duration timeout,
                    Supplier<T> defaultValueProvider) {

                final var defaultMono = nullToEmpty(defaultValueProvider);

                final ResourceManager<T> resourceManager = (lockProvider, monoProvider) -> usingWhen(
                        lockProvider,
                        lock -> monoProvider.apply(lock).timeout(timeout, defaultMono),
                        Lock::release);

                return resourceManager.using(
                        lockAcquisitionStrategy.apply(lockManager),
                        lock -> writeLockMonoFunction.apply(mono -> resourceManager.using(lockManager.toWriteLock(lock), mono)));
            }
        };
    }
}