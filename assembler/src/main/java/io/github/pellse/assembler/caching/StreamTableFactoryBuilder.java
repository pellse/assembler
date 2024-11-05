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

import io.github.pellse.assembler.ErrorHandler;
import io.github.pellse.assembler.LifeCycleEventSource;
import io.github.pellse.assembler.WindowingStrategy;
import io.github.pellse.assembler.caching.CacheFactory.CacheTransformer;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
import java.util.function.*;

import static io.github.pellse.assembler.ErrorHandler.OnErrorContinue.onErrorContinue;
import static io.github.pellse.assembler.caching.StreamTableFactory.streamTable;
import static io.github.pellse.assembler.caching.CacheEvent.toCacheEvent;

public interface StreamTableFactoryBuilder {

    static <R> WindowingStrategyBuilder<R, CacheEvent<R>> streamTableBuilder(Supplier<Flux<R>> dataSourceSupplier) {
        return streamTableBuilder(dataSourceSupplier.get());
    }

    static <R> WindowingStrategyBuilder<R, CacheEvent<R>> streamTableBuilder(Flux<R> dataSource) {
        return streamTableBuilder(dataSource, CacheEvent::updated);
    }

    static <U, R> WindowingStrategyBuilder<R, ? extends CacheEvent<R>> streamTableBuilder(
            Supplier<Flux<U>> dataSource,
            Predicate<U> isAddOrUpdateEvent,
            Function<U, R> cacheEventValueExtractor) {

        return streamTableBuilder(dataSource.get(), isAddOrUpdateEvent, cacheEventValueExtractor);
    }

    static <U, R> WindowingStrategyBuilder<R, ? extends CacheEvent<R>> streamTableBuilder(
            Flux<U> dataSource,
            Predicate<U> isAddOrUpdateEvent,
            Function<U, R> cacheEventValueExtractor) {

        return streamTableEvents(dataSource.map(toCacheEvent(isAddOrUpdateEvent, cacheEventValueExtractor)));
    }

    static <U, R, T extends CacheEvent<R>> WindowingStrategyBuilder<R, T> streamTableBuilder(Flux<U> dataSource, Function<U, T> mapper) {
        return streamTableEvents(dataSource.map(mapper));
    }

    static <R, U extends CacheEvent<R>> WindowingStrategyBuilder<R, U> streamTableEvents(Flux<U> dataSource) {
        return new Builder<>(dataSource, null, null, null, null, null);
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
            return transformer(ConcurrentCacheFactory.concurrent());
        }

        StreamTableFactoryDelegateBuilder<R> transformer(CacheTransformer<?, R, ?, ?> cacheTransformer);
    }

    interface StreamTableFactoryDelegateBuilder<R> {
        <ID, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheTransformer<ID, R, RRC, CTX> build();
    }

    record Builder<R, U extends CacheEvent<R>>(
            Flux<U> dataSource,
            WindowingStrategy<U> windowingStrategy,
            ErrorHandler errorHandler,
            Scheduler scheduler,
            LifeCycleEventSource eventSource,
            CacheTransformer<?, R, ?, ?> concurrentCacheTransformer) implements WindowingStrategyBuilder<R, U> {

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
        public StreamTableFactoryDelegateBuilder<R> transformer(CacheTransformer<?, R, ?, ?> cacheTransformer) {
            return new Builder<>(dataSource, windowingStrategy, errorHandler, scheduler, eventSource, cacheTransformer);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <ID, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheTransformer<ID, R, RRC, CTX> build() {
            return streamTable(dataSource, windowingStrategy, errorHandler, eventSource, scheduler, (CacheTransformer<ID, R, RRC, CTX>) concurrentCacheTransformer);
        }
    }
}