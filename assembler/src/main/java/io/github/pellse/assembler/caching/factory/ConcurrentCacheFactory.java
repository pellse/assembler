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

package io.github.pellse.assembler.caching.factory;

import io.github.pellse.assembler.caching.factory.CacheFactory.CacheTransformer;
import io.github.pellse.concurrent.Lock;
import io.github.pellse.concurrent.LockStrategy;
import io.github.pellse.concurrent.ReactiveGuard.ReactiveGuardBuilder;
import io.github.pellse.concurrent.ReactiveGuardEvent;
import reactor.core.scheduler.Scheduler;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static io.github.pellse.assembler.caching.ConcurrentCache.concurrentCache;
import static io.github.pellse.concurrent.ReactiveGuard.reactiveGuardBuilder;
import static io.github.pellse.concurrent.ReactiveGuardEventListener.reactiveGuardEventAdapter;

public interface ConcurrentCacheFactory {

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheTransformer<ID, R, RRC, CTX> concurrent() {
        return concurrent((LockStrategy) null);
    }

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheTransformer<ID, R, RRC, CTX> concurrent(Scheduler fetchFunctionScheduler) {
        return concurrent((LockStrategy) null, fetchFunctionScheduler);
    }

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheTransformer<ID, R, RRC, CTX> concurrent(LockStrategy lockStrategy) {
        return concurrent(lockStrategy, null);
    }

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheTransformer<ID, R, RRC, CTX> concurrent(LockStrategy lockStrategy, Scheduler fetchFunctionScheduler) {
        return concurrent(reactiveGuardBuilder().lockingStrategy(lockStrategy), fetchFunctionScheduler);
    }

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheTransformer<ID, R, RRC, CTX> concurrent(Consumer<ReactiveGuardEvent> eventConsumer) {
        return concurrent(reactiveGuardBuilder().eventListener(reactiveGuardEventAdapter(eventConsumer)));
    }

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheTransformer<ID, R, RRC, CTX> concurrent(BiConsumer<ReactiveGuardEvent, Optional<Lock<?>>> eventConsumer) {
        return concurrent(reactiveGuardBuilder().eventListener(reactiveGuardEventAdapter(eventConsumer)));
    }

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheTransformer<ID, R, RRC, CTX> concurrent(ReactiveGuardBuilder reactiveGuardBuilder) {
        return concurrent(reactiveGuardBuilder, null);
    }

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheTransformer<ID, R, RRC, CTX> concurrent(ReactiveGuardBuilder reactiveGuardBuilder, Scheduler fetchFunctionScheduler) {
        return cacheFactory -> context -> concurrentCache(cacheFactory.create(context), reactiveGuardBuilder, fetchFunctionScheduler);
    }
}