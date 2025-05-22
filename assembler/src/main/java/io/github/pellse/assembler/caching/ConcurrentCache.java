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

package io.github.pellse.assembler.caching;

import io.github.pellse.concurrent.LockStrategy;
import io.github.pellse.concurrent.ReactiveGuard;
import io.github.pellse.concurrent.ReactiveGuard.ReactiveGuardBuilder;
import io.github.pellse.util.function.Function3;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.util.Map;

import static io.github.pellse.concurrent.ReactiveGuard.reactiveGuardBuilder;
import static java.util.Objects.requireNonNullElseGet;

public interface ConcurrentCache<ID, RRC> extends Cache<ID, RRC> {

    static <ID, RRC> ConcurrentCache<ID, RRC> concurrentCache(Cache<ID, RRC> delegateCache) {
        return concurrentCache(delegateCache, (ReactiveGuardBuilder) null);
    }

    static <ID, RRC> ConcurrentCache<ID, RRC> concurrentCache(Cache<ID, RRC> delegateCache, Scheduler fetchFunctionScheduler) {
        return concurrentCache(delegateCache, (ReactiveGuardBuilder) null, fetchFunctionScheduler);
    }

    static <ID, RRC> ConcurrentCache<ID, RRC> concurrentCache(Cache<ID, RRC> delegateCache, LockStrategy lockStrategy) {
        return concurrentCache(delegateCache, reactiveGuardBuilder().lockingStrategy(lockStrategy));
    }

    static <ID, RRC> ConcurrentCache<ID, RRC> concurrentCache(Cache<ID, RRC> delegateCache, LockStrategy lockStrategy, Scheduler fetchFunctionScheduler) {
        return concurrentCache(delegateCache, reactiveGuardBuilder().lockingStrategy(lockStrategy), fetchFunctionScheduler);
    }

    static <ID, RRC> ConcurrentCache<ID, RRC> concurrentCache(Cache<ID, RRC> delegateCache, ReactiveGuardBuilder reactiveGuardBuilder) {
        return concurrentCache(delegateCache, reactiveGuardBuilder, null);
    }

    static <ID, RRC> ConcurrentCache<ID, RRC> concurrentCache(Cache<ID, RRC> delegateCache, ReactiveGuardBuilder reactiveGuardBuilder, Scheduler fetchFunctionScheduler) {

        if (delegateCache instanceof ConcurrentCache<ID, RRC> concurrentCache) {
            return concurrentCache;
        }

        final var guard = requireNonNullElseGet(reactiveGuardBuilder, ReactiveGuard::reactiveGuardBuilder)
                .build();

        return new ConcurrentCache<>() {

            @Override
            public Mono<Map<ID, RRC>> getAll(Iterable<ID> ids) {
                return guard.withReadLock(delegateCache.getAll(ids), Map::of);
            }

            @Override
            public Mono<Map<ID, RRC>> computeAll(Iterable<ID> ids, FetchFunction<ID, RRC> fetchFunction) {
                return guard.withReadLock(delegateCache.computeAll(ids, idsToFetch -> fetchFunction.apply(idsToFetch, fetchFunctionScheduler)), Map::of);
            }

            @Override
            public Mono<?> putAll(Map<ID, RRC> map) {
                return guard.withLock(delegateCache.putAll(map));
            }

            @Override
            public <UUC> Mono<?> putAllWith(Map<ID, UUC> map, Function3<ID, RRC, UUC, RRC> mergeFunction) {
                return guard.withLock(delegateCache.putAllWith(map, mergeFunction));
            }

            @Override
            public Mono<?> removeAll(Map<ID, RRC> map) {
                return guard.withLock(delegateCache.removeAll(map));
            }

            @Override
            public Mono<?> updateAll(Map<ID, RRC> mapToAdd, Map<ID, RRC> mapToRemove) {
                return guard.withLock(delegateCache.updateAll(mapToAdd, mapToRemove));
            }
        };
    }
}
