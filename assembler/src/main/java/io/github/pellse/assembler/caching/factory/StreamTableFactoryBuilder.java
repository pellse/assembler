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

import io.github.pellse.assembler.ErrorHandler;
import io.github.pellse.assembler.LifeCycleEventSource;
import io.github.pellse.assembler.WindowingStrategy;
import io.github.pellse.assembler.caching.factory.CacheContext.OneToManyCacheContext;
import io.github.pellse.assembler.caching.factory.CacheContext.OneToOneCacheContext;
import io.github.pellse.assembler.caching.CacheEvent;
import io.github.pellse.concurrent.Lock;
import io.github.pellse.concurrent.LockStrategy;
import io.github.pellse.concurrent.ReactiveGuard.ReactiveGuardBuilder;
import io.github.pellse.concurrent.ReactiveGuardEvent;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.*;

import static io.github.pellse.assembler.ErrorHandler.OnErrorContinue.onErrorContinue;
import static io.github.pellse.assembler.caching.factory.StreamTableFactory.streamTable;
import static io.github.pellse.assembler.caching.CacheEvent.toCacheEvent;
import static io.github.pellse.concurrent.ReactiveGuard.reactiveGuardBuilder;
import static io.github.pellse.concurrent.ReactiveGuardEventListener.reactiveGuardEventAdapter;
import static java.util.function.Function.identity;

public interface StreamTableFactoryBuilder {

    static <R> WindowingStrategyBuilder<R, ? extends CacheEvent<R>> streamTableBuilder(Flux<R> dataSource) {
        return streamTableBuilder(dataSource, __ -> true);
    }

    static <R> WindowingStrategyBuilder<R, ? extends CacheEvent<R>> streamTableBuilder(
            Flux<R> dataSource,
            Predicate<R> isAddOrUpdateEvent) {

        return streamTableBuilder(dataSource, isAddOrUpdateEvent, identity());
    }

    static <U, R> WindowingStrategyBuilder<R, ? extends CacheEvent<R>> streamTableBuilder(
            Flux<U> dataSource,
            Predicate<U> isAddOrUpdateEvent,
            Function<U, R> cacheEventValueExtractor) {

        return new Builder<>(dataSource.map(toCacheEvent(isAddOrUpdateEvent, cacheEventValueExtractor)), null, null, null, null, null);
    }

    interface WindowingStrategyBuilder<R, U extends CacheEvent<R>> extends ConfigBuilder<R> {

        default ConfigBuilder<R> maxWindowSize(int maxWindowSize) {
            return windowingStrategy(flux -> flux.window(maxWindowSize));
        }

        default ConfigBuilder<R> maxWindowTime(Duration maxWindowTime) {
            return windowingStrategy(flux -> flux.window(maxWindowTime));
        }

        default ConfigBuilder<R> maxWindowSizeAndTime(int maxWindowSize, Duration maxWindowTime) {
            return windowingStrategy(flux -> flux.windowTimeout(maxWindowSize, maxWindowTime));
        }

        ConfigBuilder<R> windowingStrategy(WindowingStrategy<U> windowingStrategy);
    }

    interface ConfigBuilder<R> extends LifeCycleEventSourceBuilder<R> {

        default LifeCycleEventSourceBuilder<R> errorHandler(Consumer<Throwable> errorConsumer) {
            return errorHandler(onErrorContinue(errorConsumer));
        }

        default LifeCycleEventSourceBuilder<R> errorHandler(BiConsumer<Throwable, Object> errorConsumer) {
            return errorHandler(onErrorContinue(errorConsumer));
        }

        LifeCycleEventSourceBuilder<R> errorHandler(ErrorHandler errorHandler);
    }

    interface LifeCycleEventSourceBuilder<R> extends SchedulerBuilder<R> {
        SchedulerBuilder<R> lifeCycleEventSource(LifeCycleEventSource eventSource);
    }

    interface SchedulerBuilder<R> extends CacheTransformerBuilder<R> {
        CacheTransformerBuilder<R> scheduler(Scheduler scheduler);
    }

    interface CacheTransformerBuilder<R> extends StreamTableFactoryDelegateBuilder<R> {

        default StreamTableFactoryDelegateBuilder<R> concurrent() {
            return concurrent((LockStrategy) null);
        }

        default StreamTableFactoryDelegateBuilder<R> concurrent(Scheduler fetchFunctionScheduler) {
            return concurrent((LockStrategy) null, fetchFunctionScheduler);
        }

        default StreamTableFactoryDelegateBuilder<R> concurrent(LockStrategy lockStrategy) {
            return concurrent(reactiveGuardBuilder().lockingStrategy(lockStrategy));
        }

        default StreamTableFactoryDelegateBuilder<R> concurrent(LockStrategy lockStrategy, Scheduler fetchFunctionScheduler) {
            return concurrent(reactiveGuardBuilder().lockingStrategy(lockStrategy), fetchFunctionScheduler);
        }

        default StreamTableFactoryDelegateBuilder<R> concurrent(Consumer<ReactiveGuardEvent> eventConsumer) {
            return concurrent(reactiveGuardBuilder().eventListener(reactiveGuardEventAdapter(eventConsumer)));
        }

        default StreamTableFactoryDelegateBuilder<R> concurrent(BiConsumer<ReactiveGuardEvent, Optional<Lock<?>>> eventConsumer) {
            return concurrent(reactiveGuardBuilder().eventListener(reactiveGuardEventAdapter(eventConsumer)));
        }

        default StreamTableFactoryDelegateBuilder<R> concurrent(ReactiveGuardBuilder reactiveGuardBuilder) {
            return concurrent(reactiveGuardBuilder, null);
        }

        default StreamTableFactoryDelegateBuilder<R> concurrent(ReactiveGuardBuilder reactiveGuardBuilder, Scheduler fetchFunctionScheduler) {
            return cacheTransformer(ConcurrentCacheFactory.concurrent(reactiveGuardBuilder, fetchFunctionScheduler));
        }

        default StreamTableFactoryDelegateBuilder<R> oneToOneCacheTransformer(CacheTransformer<Object, R, R, OneToOneCacheContext<Object, R>> cacheTransformer) {
            return cacheTransformer(CacheTransformer.oneToOneCacheTransformer(cacheTransformer));
        }

        default StreamTableFactoryDelegateBuilder<R> oneToManyCacheTransformer(CacheTransformer<Object, R, List<R>, OneToManyCacheContext<Object, Object, R>> cacheTransformer) {
            return cacheTransformer(CacheTransformer.oneToManyCacheTransformer(cacheTransformer));
        }

        default StreamTableFactoryDelegateBuilder<R> defaultCacheTransformer() {
            return cacheTransformer(CacheTransformer.defaultCacheTransformer());
        }

        StreamTableFactoryDelegateBuilder<R> cacheTransformer(CacheTransformer<?, R, ?, ?> cacheTransformer);
    }

    interface StreamTableFactoryDelegateBuilder<R> {

        <ID, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheTransformer<ID, R, RRC, CTX> build();

        default <ID> CacheTransformer<ID, R, R, OneToOneCacheContext<ID, R>> build(@SuppressWarnings("unused") Class<ID> idClass) {
            return build();
        }

        default <ID, EID> CacheTransformer<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>> build(
                @SuppressWarnings("unused") Class<ID> idClass,
                @SuppressWarnings("unused") Class<EID> elementIdClass) {

            return build();
        }
    }

    record Builder<R, U extends CacheEvent<R>>(
            Flux<U> dataSource,
            WindowingStrategy<U> windowingStrategy,
            ErrorHandler errorHandler,
            Scheduler scheduler,
            LifeCycleEventSource eventSource,
            CacheTransformer<?, R, ?, ?> cacheTransformer) implements WindowingStrategyBuilder<R, U> {

        @Override
        public ConfigBuilder<R> windowingStrategy(WindowingStrategy<U> windowingStrategy) {
            return new Builder<>(dataSource, windowingStrategy, null, null, null, null);
        }

        @Override
        public LifeCycleEventSourceBuilder<R> errorHandler(ErrorHandler errorHandler) {
            return new Builder<>(dataSource, windowingStrategy, errorHandler, null, null, null);
        }

        @Override
        public SchedulerBuilder<R> lifeCycleEventSource(LifeCycleEventSource eventSource) {
            return new Builder<>(dataSource, windowingStrategy, errorHandler, null, eventSource, null);
        }

        @Override
        public CacheTransformerBuilder<R> scheduler(Scheduler scheduler) {
            return new Builder<>(dataSource, windowingStrategy, errorHandler, scheduler, eventSource, null);
        }

        @Override
        public StreamTableFactoryDelegateBuilder<R> cacheTransformer(CacheTransformer<?, R, ?, ?> cacheTransformer) {
            return new Builder<>(dataSource, windowingStrategy, errorHandler, scheduler, eventSource, cacheTransformer);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <ID, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheTransformer<ID, R, RRC, CTX> build() {
            return streamTable(dataSource, windowingStrategy, errorHandler, eventSource, scheduler, (CacheTransformer<ID, R, RRC, CTX>) cacheTransformer);
        }
    }
}