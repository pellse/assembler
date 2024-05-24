package io.github.pellse.concurrent;

import reactor.core.publisher.Mono;
import java.util.function.Function;

import static reactor.core.publisher.Mono.*;

public interface ReentrantExecutor {

    default <T> Mono<T> withReadLock(Mono<T> mono) {
        return withReadLock(__ -> mono);
    }

    <T> Mono<T> withReadLock(Function<WriteLockExecutor, Mono<T>> monoProvider);

    <T> Mono<T> withWriteLock(Mono<T> mono);

    @FunctionalInterface
    interface WriteLockExecutor {
        <U> Mono<U> withWriteLock(Mono<U> mono);
    }

    static ReentrantExecutor create() {

        final var lockManager = new LockManager();

        return new ReentrantExecutor() {

            @Override
            public <T> Mono<T> withReadLock(Function<WriteLockExecutor, Mono<T>> monoProvider) {
                return with(monoProvider, LockManager::acquireReadLock);
            }

            @Override
            public <T> Mono<T> withWriteLock(Mono<T> mono) {
                return with(__ -> mono, LockManager::acquireWriteLock);
            }

            private <T> Mono<T> with(Function<WriteLockExecutor, Mono<T>> monoProvider, Function<LockManager, Mono<LockManager.Lock>> acquireLockStrategy) {
                return usingWhen(
                        acquireLockStrategy.apply(lockManager),
                        lock -> monoProvider.apply(new WriteLockExecutor() {
                                    @Override
                                    public <U> Mono<U> withWriteLock(Mono<U> mono) {
                                        return usingWhen(
                                                lockManager.convertToWriteLock(lock),
                                                writeLock -> mono,
                                                writeLock -> fromRunnable(writeLock::release)
                                        );
                                    }
                                }),
                        lock -> fromRunnable(lock::release)
                );
            }
        };
    }
}