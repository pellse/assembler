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
                                Lock::release
                        )).timeout(timeout, defaultMono),
                        Lock::release
                );
            }
        };
    }
}