/*
 * Copyright 2023 Sebastien Pelletier
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

package io.github.pellse.reactive.assembler.caching;

import io.github.pellse.reactive.assembler.LifeCycleEventSource;
import io.github.pellse.reactive.assembler.LifeCycleEventSource.LifeCycleEventListener;
import io.github.pellse.reactive.assembler.caching.AutoCacheFactory.ErrorHandler;
import io.github.pellse.reactive.assembler.caching.AutoCacheFactory.WindowingStrategy;
import io.github.pellse.reactive.assembler.caching.CacheFactory.CacheTransformer;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.github.pellse.reactive.assembler.caching.AutoCacheFactory.OnErrorStop.onErrorStop;
import static io.github.pellse.reactive.assembler.caching.AutoCacheFactory.defaultWindowingStrategy;
import static io.github.pellse.reactive.assembler.caching.CacheEvent.toCacheEvent;

public interface AutoCacheFactoryBuilder {

    static <R> WindowingStrategyBuilder<R, CacheEvent<R>> autoCache(Flux<R> dataSource) {
        return autoCache(dataSource, CacheEvent::updated);
    }

    static <U, R> WindowingStrategyBuilder<R, ? extends CacheEvent<R>> autoCache(
            Flux<U> dataSource,
            Predicate<U> isUpdated,
            Function<U, R> cacheEventValueExtractor) {
        return autoCacheEvents(dataSource.map(toCacheEvent(isUpdated, cacheEventValueExtractor)));
    }

    static <U, R, T extends CacheEvent<R>> WindowingStrategyBuilder<R, T> autoCache(Flux<U> dataSource, Function<U, T> mapper) {
        return autoCacheEvents(dataSource.map(mapper));
    }

    static <R, T extends CacheEvent<R>> WindowingStrategyBuilder<R, T> autoCacheEvents(Flux<T> dataSource) {
        return new Builder<>(dataSource);
    }

    interface WindowingStrategyBuilder<R, T extends CacheEvent<R>> extends ConfigBuilder<R> {
        ConfigBuilder<R> maxWindowSize(int maxWindowSize);

        ConfigBuilder<R> maxWindowTime(Duration maxWindowTime);

        ConfigBuilder<R> maxWindowSizeAndTime(int maxWindowSize, Duration maxWindowTime);

        ConfigBuilder<R> windowingStrategy(WindowingStrategy<T> windowingStrategy);
    }

    interface ConfigBuilder<R> extends LifeCycleEventSourceBuilder<R> {
        LifeCycleEventSourceBuilder<R> errorHandler(ErrorHandler errorHandler);
    }

    interface LifeCycleEventSourceBuilder<R> extends SchedulerBuilder<R> {
        SchedulerBuilder<R> lifeCycleEventSource(LifeCycleEventSource eventSource);
    }

    interface SchedulerBuilder<R> extends AutoCacheFactoryDelegateBuilder<R> {
        AutoCacheFactoryDelegateBuilder<R> scheduler(Scheduler scheduler);
    }

    interface AutoCacheFactoryDelegateBuilder<R> {
        <ID, RRC> CacheTransformer<ID, R, RRC> build();
    }

    class Builder<R, T extends CacheEvent<R>> implements WindowingStrategyBuilder<R, T> {

        private final Flux<T> dataSource;
        private WindowingStrategy<T> windowingStrategy = defaultWindowingStrategy();
        private ErrorHandler errorHandler = onErrorStop();

        private Scheduler scheduler;

        private LifeCycleEventSource eventSource = LifeCycleEventListener::start;

        Builder(Flux<T> dataSource) {
            this.dataSource = dataSource;
        }

        @Override
        public ConfigBuilder<R> maxWindowSize(int maxWindowSize) {
            return windowingStrategy(flux -> flux.window(maxWindowSize));
        }

        @Override
        public ConfigBuilder<R> maxWindowTime(Duration maxWindowTime) {
            return windowingStrategy(flux -> flux.window(maxWindowTime));
        }

        @Override
        public ConfigBuilder<R> maxWindowSizeAndTime(int maxWindowSize, Duration maxWindowTime) {
            return windowingStrategy(flux -> flux.windowTimeout(maxWindowSize, maxWindowTime));
        }

        @Override
        public ConfigBuilder<R> windowingStrategy(WindowingStrategy<T> windowingStrategy) {
            this.windowingStrategy = windowingStrategy;
            return this;
        }

        @Override
        public LifeCycleEventSourceBuilder<R> errorHandler(ErrorHandler errorHandler) {
            this.errorHandler = errorHandler;
            return this;
        }

        @Override
        public SchedulerBuilder<R> lifeCycleEventSource(LifeCycleEventSource eventSource) {
            this.eventSource = eventSource;
            return this;
        }

        @Override
        public AutoCacheFactoryDelegateBuilder<R> scheduler(Scheduler scheduler) {
            this.scheduler = scheduler;
            return this;
        }

        @Override
        public <ID, RRC> CacheTransformer<ID, R, RRC> build() {
            return AutoCacheFactory.autoCache(dataSource, windowingStrategy, errorHandler, eventSource, scheduler);
        }
    }
}
