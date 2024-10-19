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

import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.github.pellse.util.reactive.ReactiveUtils.nullToEmpty;
import static java.lang.Long.MAX_VALUE;
import static java.time.Duration.ofSeconds;
import static reactor.core.publisher.Mono.*;

public interface ReentrantExecutor {

    @FunctionalInterface
    interface WriteLockExecutor<U> {
        default Mono<U> withLock(Supplier<Mono<U>> monoSupplier) {
            return withLock(defer(monoSupplier));
        }

        Mono<U> withLock(Mono<U> mono);
    }

    Duration DEFAULT_TIMEOUT = ofSeconds(10);

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

    <T> Mono<T> withReadLock(Function<WriteLockExecutor<T>, Mono<T>> writeLockMonoFunction, Duration timeout, Supplier<T> defaultValueProvider);

    <T> Mono<T> withWriteLock(Mono<T> mono, Duration timeout, Supplier<T> defaultValueProvider);

    static ReentrantExecutor create() {
        return create(MAX_VALUE, MAX_VALUE);
    }

    static ReentrantExecutor create(long readQueueCapacity, long writeQueueCapacity) {

        final var lockManager = new LockManager(readQueueCapacity, writeQueueCapacity);

        return new ReentrantExecutor() {

            @FunctionalInterface
            interface ResourceManager<T> {
                default Mono<T> using(Mono<Lock> lockProvider, Mono<T> mono) {
                    return using(lockProvider, __ -> mono);
                }

                Mono<T> using(Mono<Lock> lockProvider, Function<Lock, Mono<T>> monoSupplier);
            }

            @Override
            public <T> Mono<T> withReadLock(Function<WriteLockExecutor<T>, Mono<T>> writeLockMonoFunction, Duration timeout, Supplier<T> defaultValueProvider) {
                return with(writeLockMonoFunction, LockManager::acquireReadLock, timeout, defaultValueProvider);
            }

            @Override
            public <T> Mono<T> withWriteLock(Mono<T> mono, Duration timeout, Supplier<T> defaultValueProvider) {
                return with(__ -> mono, LockManager::acquireWriteLock, timeout, defaultValueProvider);
            }

            private <T> Mono<T> with(
                    Function<WriteLockExecutor<T>, Mono<T>> writeLockMonoFunction,
                    Function<LockManager, Mono<Lock>> lockAcquisitionStrategy,
                    Duration timeout,
                    Supplier<T> defaultValueProvider) {

                final Mono<T> defaultMono = nullToEmpty(defaultValueProvider);

                final ResourceManager<T> resourceManager = (lockProvider, monoSupplier) -> usingWhen(
                        lockProvider,
                        lock -> monoSupplier.apply(lock).timeout(timeout, defaultMono),
                        Lock::release);

                return resourceManager.using(
                        lockAcquisitionStrategy.apply(lockManager),
                        lock -> writeLockMonoFunction.apply(mono -> resourceManager.using(lockManager.toWriteLock(lock), mono)));
            }
        };
    }
}