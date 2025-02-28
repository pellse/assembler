/*
 * Copyright 2024 Sebastien Pelletier
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

import io.github.pellse.concurrent.CoreLock.ReadLock;
import io.github.pellse.concurrent.CoreLock.WriteLock;
import io.github.pellse.concurrent.LockStrategy.LockAcquisitionException;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static io.github.pellse.util.ObjectUtils.get;
import static io.github.pellse.util.reactive.ReactiveUtils.nullToEmpty;
import static java.time.Duration.ofSeconds;
import static java.util.Objects.requireNonNullElseGet;
import static java.util.Optional.ofNullable;
import static reactor.core.Exceptions.isRetryExhausted;
import static reactor.core.publisher.Mono.*;

public interface ReactiveGuard {

    Duration DEFAULT_TIMEOUT = ofSeconds(10);

    <T> Mono<T> withReadLock(Mono<T> mono, Duration timeout, Supplier<T> defaultValueProvider);

    <T> Mono<T> withReadLock(Function<ReactiveWriteGuard<T>, Mono<T>> writeLockMonoFunction, Duration timeout, Supplier<T> defaultValueProvider);

    <T> Mono<T> withLock(Mono<T> mono, Duration timeout, Supplier<T> defaultValueProvider);

    default <T> Mono<T> withReadLock(Supplier<Mono<T>> monoSupplier) {
        return withReadLock(defer(monoSupplier));
    }

    default <T> Mono<T> withReadLock(Mono<T> mono) {
        return withReadLock(mono, null);
    }

    default <T> Mono<T> withReadLock(Supplier<Mono<T>> monoSupplier, Supplier<T> defaultValueProvider) {
        return withReadLock(defer(monoSupplier), defaultValueProvider);
    }

    default <T> Mono<T> withReadLock(Mono<T> mono, Supplier<T> defaultValueProvider) {
        return withReadLock(mono, DEFAULT_TIMEOUT, defaultValueProvider);
    }

    default <T> Mono<T> withReadLock(Function<ReactiveWriteGuard<T>, Mono<T>> writeLockMonoFunction) {
        return withReadLock(writeLockMonoFunction, null);
    }

    default <T> Mono<T> withReadLock(Function<ReactiveWriteGuard<T>, Mono<T>> writeLockMonoFunction, Supplier<T> defaultValueProvider) {
        return withReadLock(writeLockMonoFunction, DEFAULT_TIMEOUT, defaultValueProvider);
    }

    default <T> Mono<T> withReadLock(Supplier<Mono<T>> monoSupplier, Duration timeout, Supplier<T> defaultValueProvider) {
        return withReadLock(defer(monoSupplier), timeout, defaultValueProvider);
    }

    default <T> Mono<T> withLock(Supplier<Mono<T>> monoSupplier) {
        return withLock(defer(monoSupplier));
    }

    default <T> Mono<T> withLock(Mono<T> mono) {
        return withLock(mono, null);
    }

    default <T> Mono<T> withLock(Supplier<Mono<T>> monoSupplier, Supplier<T> defaultValueProvider) {
        return withLock(defer(monoSupplier), defaultValueProvider);
    }

    default <T> Mono<T> withLock(Mono<T> mono, Supplier<T> defaultValueProvider) {
        return withLock(mono, DEFAULT_TIMEOUT, defaultValueProvider);
    }

    default <T> Mono<T> withLock(Supplier<Mono<T>> monoSupplier, Duration timeout, Supplier<T> defaultValueProvider) {
        return withLock(defer(monoSupplier), timeout, defaultValueProvider);
    }

    static ReactiveGuard createReactiveGuard() {
        return createReactiveGuard((LockStrategy) null);
    }

    static ReactiveGuard createReactiveGuard(LockStrategy lockStrategy) {
        return reactiveGuardBuilder()
                .lockingStrategy(lockStrategy)
                .build();
    }

    static ReactiveGuardBuilder reactiveGuardBuilder() {
        return new ReactiveGuardBuilder();
    }

    private static ReactiveGuard createReactiveGuard(ReactiveGuardBuilder reactiveGuardBuilder) {

        final LockStrategy lockStrategy = requireNonNullElseGet(reactiveGuardBuilder.lockingStrategy, CASLockStrategy::new);
        final ReactiveGuardEventListener<Object> eventListener = requireNonNullElseGet(reactiveGuardBuilder.eventListener, ReactiveGuardEventListener::defaultReactiveGuardListener);
        final Optional<Scheduler> timeoutScheduler = ofNullable(reactiveGuardBuilder.timeoutScheduler);

        return new ReactiveGuard() {

            @Override
            public <T> Mono<T> withReadLock(Mono<T> mono, Duration timeout, Supplier<T> defaultValueProvider) {
                return with(mono, LockStrategy::acquireReadLock, timeout, defaultValueProvider);
            }

            @Override
            public <T> Mono<T> withReadLock(Function<ReactiveWriteGuard<T>, Mono<T>> writeLockMonoFunction, Duration timeout, Supplier<T> defaultValueProvider) {
                return with(writeLockMonoFunction, LockStrategy::acquireReadLock, timeout, defaultValueProvider);
            }

            @Override
            public <T> Mono<T> withLock(Mono<T> mono, Duration timeout, Supplier<T> defaultValueProvider) {
                return with(mono, LockStrategy::acquireWriteLock, timeout, defaultValueProvider);
            }

            private <T> Mono<T> with(
                    Mono<T> mono,
                    Function<LockStrategy, Mono<? extends Lock<?>>> lockAcquisitionStrategy,
                    Duration timeout,
                    Supplier<T> defaultValueProvider) {

                return executeWithLock(lockAcquisitionStrategy.apply(lockStrategy), __ -> mono, timeout, defaultValueProvider);
            }

            private <T> Mono<T> with(
                    Function<ReactiveWriteGuard<T>, Mono<T>> writeLockMonoFunction,
                    Function<LockStrategy, Mono<? extends Lock<?>>> lockAcquisitionStrategy,
                    Duration timeout,
                    Supplier<T> defaultValueProvider) {

                @FunctionalInterface
                interface ResourceManager<T> {
                    Mono<T> using(Mono<? extends Lock<?>> lockProvider, Function<Lock<?>, Mono<T>> monoProvider);

                    default Mono<T> using(Mono<? extends Lock<?>> lockProvider, Mono<T> mono) {
                        return using(lockProvider, __ -> mono);
                    }
                }

                final ResourceManager<T> resourceManager = (lockMono, monoProvider) -> executeWithLock(lockMono, monoProvider, timeout, defaultValueProvider);

                return resourceManager.using(
                        lockAcquisitionStrategy.apply(lockStrategy),
                        lock -> writeLockMonoFunction.apply(mono -> resourceManager.using(lockStrategy.toWriteLock(lock), mono)));
            }

            private <T> Mono<T> executeWithLock(
                    Mono<? extends Lock<?>> lockProvider,
                    Function<Lock<?>, Mono<T>> monoProvider,
                    Duration timeout,
                    Supplier<T> defaultValueProvider) {

                return usingWhen(
                        lockProvider
                                .doOnNext(this::onLockAcquired)
                                .doOnError(eventListener::onLockAcquisitionFailed),
                        lock -> executeWithTimeout(lock, monoProvider, timeout, defaultValueProvider)
                                .doOnSubscribe(__ -> eventListener.onBeforeTaskExecution(lock))
                                .doOnNext(value -> eventListener.onAfterTaskExecution(lock, value))
                                .doOnError(t -> eventListener.onTaskExecutionFailed(lock, t))
                                .doOnCancel(() -> eventListener.onTaskExecutionCancelled(lock)),
                        lock -> lock.release()
                                .doOnSuccess(__ -> eventListener.onLockReleased(lock))
                                .doOnError(t -> eventListener.onLockReleaseFailed(lock, t))
                                .doOnCancel(() -> eventListener.onLockReleaseCancelled(lock)))
                        .transform(managedMono -> errorHandler(managedMono, defaultValueProvider));
            }

            private void onLockAcquired(Lock<?> lock) {
                if (lock.delegate() instanceof WriteLock writeLock && lock.outerLock() instanceof ReadLock) {
                    eventListener.onLockUpgraded(writeLock);
                } else {
                    eventListener.onLockAcquired(lock);
                }
            }

            private <T> Mono<T> executeWithTimeout(
                    Lock<?> lock,
                    Function<Lock<?>, Mono<T>> monoProvider,
                    Duration timeout,
                    Supplier<T> defaultValueProvider) {

                final UnaryOperator<Mono<T>> runWithTimeout = mono ->
                        timeoutScheduler
                                .map(scheduler -> mono.timeout(timeout,
                                        nullToEmpty(defaultValueProvider)
                                                .doOnSubscribe(__ -> eventListener.onTaskExecutionTimeout(lock)), scheduler))
                                .orElse(mono);

                return monoProvider.apply(lock)
                        .transform(runWithTimeout);
            }
        };
    }

    private static <T> Mono<T> errorHandler(Mono<T> mono, Supplier<T> defaultValueProvider) {
        return mono.onErrorResume(ReactiveGuard::isLockAcquisitionFailed, get(nullToEmpty(defaultValueProvider)));
    }

    private static boolean isLockAcquisitionFailed(Throwable t) {
        return t instanceof LockAcquisitionException || isRetryExhausted(t);
    }

    class ReactiveGuardBuilder {
        private LockStrategy lockingStrategy;
        private Scheduler timeoutScheduler;
        private ReactiveGuardEventListener<Object> eventListener;

        public ReactiveGuardBuilder lockingStrategy(LockStrategy lockingStrategy) {
            this.lockingStrategy = lockingStrategy;
            return this;
        }

        public ReactiveGuardBuilder timeoutScheduler(Scheduler timeoutScheduler) {
            this.timeoutScheduler = timeoutScheduler;
            return this;
        }

        public ReactiveGuardBuilder eventListener(ReactiveGuardEventListener<Object> eventListener) {
            this.eventListener = eventListener;
            return this;
        }

        public ReactiveGuard build() {
            return createReactiveGuard(this);
        }
    }

    @FunctionalInterface
    interface ReactiveWriteGuard<U> {
        Mono<U> withLock(Mono<U> mono);

        default Mono<U> withLock(Supplier<Mono<U>> monoSupplier) {
            return withLock(defer(monoSupplier));
        }
    }
}