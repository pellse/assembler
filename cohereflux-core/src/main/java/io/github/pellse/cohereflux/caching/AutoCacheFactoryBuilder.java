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

package io.github.pellse.cohereflux.caching;

import io.github.pellse.cohereflux.LifeCycleEventSource;
import io.github.pellse.cohereflux.caching.AutoCacheFactory.ErrorHandler;
import io.github.pellse.cohereflux.caching.AutoCacheFactory.WindowingStrategy;
import io.github.pellse.cohereflux.caching.CacheFactory.CacheTransformer;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.util.retry.RetryBackoffSpec;
import reactor.util.retry.RetrySpec;

import java.time.Duration;
import java.util.function.*;

import static io.github.pellse.cohereflux.caching.AutoCacheFactory.OnErrorContinue.onErrorContinue;
import static io.github.pellse.cohereflux.caching.AutoCacheFactory.autoCache;
import static io.github.pellse.cohereflux.caching.CacheEvent.toCacheEvent;
import static reactor.util.retry.Retry.*;

public interface AutoCacheFactoryBuilder {

    static <R> WindowingStrategyBuilder<R, CacheEvent<R>> autoCacheBuilder(Supplier<Flux<R>> dataSourceSupplier) {
        return autoCacheBuilder(dataSourceSupplier.get());
    }

    static <R> WindowingStrategyBuilder<R, CacheEvent<R>> autoCacheBuilder(Flux<R> dataSource) {
        return autoCacheBuilder(dataSource, CacheEvent::updated);
    }

    static <U, R> WindowingStrategyBuilder<R, ? extends CacheEvent<R>> autoCacheBuilder(
            Supplier<Flux<U>> dataSource,
            Predicate<U> isAddOrUpdateEvent,
            Function<U, R> cacheEventValueExtractor) {
        return autoCacheBuilder(dataSource.get(), isAddOrUpdateEvent, cacheEventValueExtractor);
    }

    static <U, R> WindowingStrategyBuilder<R, ? extends CacheEvent<R>> autoCacheBuilder(
            Flux<U> dataSource,
            Predicate<U> isAddOrUpdateEvent,
            Function<U, R> cacheEventValueExtractor) {
        return autoCacheEvents(dataSource.map(toCacheEvent(isAddOrUpdateEvent, cacheEventValueExtractor)));
    }

    static <U, R, T extends CacheEvent<R>> WindowingStrategyBuilder<R, T> autoCacheBuilder(Flux<U> dataSource, Function<U, T> mapper) {
        return autoCacheEvents(dataSource.map(mapper));
    }

    static <R, U extends CacheEvent<R>> WindowingStrategyBuilder<R, U> autoCacheEvents(Flux<U> dataSource) {
        return new Builder<>(dataSource);
    }

    interface WindowingStrategyBuilder<R, U extends CacheEvent<R>> extends ConfigBuilder<R> {
        ConfigBuilder<R> maxWindowSize(int maxWindowSize);

        ConfigBuilder<R> maxWindowTime(Duration maxWindowTime);

        ConfigBuilder<R> maxWindowSizeAndTime(int maxWindowSize, Duration maxWindowTime);

        ConfigBuilder<R> windowingStrategy(WindowingStrategy<U> windowingStrategy);
    }

    interface ConfigBuilder<R> extends LifeCycleEventSourceBuilder<R> {
        LifeCycleEventSourceBuilder<R> errorHandler(Consumer<Throwable> errorConsumer);

        LifeCycleEventSourceBuilder<R> errorHandler(BiConsumer<Throwable, Object> errorConsumer);

        LifeCycleEventSourceBuilder<R> errorHandler(ErrorHandler errorHandler);
    }

    interface LifeCycleEventSourceBuilder<R> extends SchedulerBuilder<R> {
        SchedulerBuilder<R> lifeCycleEventSource(LifeCycleEventSource eventSource);
    }

    interface SchedulerBuilder<R> extends RetryStrategyBuilder<R> {
        RetryStrategyBuilder<R> scheduler(Scheduler scheduler);
    }

    interface RetryStrategyBuilder<R> extends AutoCacheFactoryDelegateBuilder<R> {

        AutoCacheFactoryDelegateBuilder<R> maxRetryStrategy(long maxAttempts);

        AutoCacheFactoryDelegateBuilder<R> backoffRetryStrategy(long maxAttempts, Duration minBackoff);

        AutoCacheFactoryDelegateBuilder<R> backoffRetryStrategy(long maxAttempts, Duration minBackoff, Duration maxBackoff);

        AutoCacheFactoryDelegateBuilder<R> fixedDelayRetryStrategy(long maxAttempts, Duration fixedDelay);

        AutoCacheFactoryDelegateBuilder<R> retryStrategy(RetrySpec retrySpec);

        AutoCacheFactoryDelegateBuilder<R> retryStrategy(RetryBackoffSpec retryBackoffSpec);
    }

    interface AutoCacheFactoryDelegateBuilder<R> {
        <ID, RRC> CacheTransformer<ID, R, RRC> build();
    }

    class Builder<R, U extends CacheEvent<R>> implements WindowingStrategyBuilder<R, U> {

        private final Flux<U> dataSource;
        private WindowingStrategy<U> windowingStrategy;
        private ErrorHandler errorHandler;
        private Scheduler scheduler;
        private LifeCycleEventSource eventSource;
        private CacheTransformer<?, R, ?> cacheTransformer;

        private Builder(Flux<U> dataSource) {
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
        public ConfigBuilder<R> windowingStrategy(WindowingStrategy<U> windowingStrategy) {
            this.windowingStrategy = windowingStrategy;
            return this;
        }

        @Override
        public LifeCycleEventSourceBuilder<R> errorHandler(Consumer<Throwable> errorConsumer) {
            return errorHandler(onErrorContinue(errorConsumer));
        }

        @Override
        public LifeCycleEventSourceBuilder<R> errorHandler(BiConsumer<Throwable, Object> errorConsumer) {
            return errorHandler(onErrorContinue(errorConsumer));
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
        public RetryStrategyBuilder<R> scheduler(Scheduler scheduler) {
            this.scheduler = scheduler;
            return this;
        }

        @Override
        public AutoCacheFactoryDelegateBuilder<R> maxRetryStrategy(long maxAttempts) {
            return retryStrategy(max(maxAttempts));
        }

        @Override
        public AutoCacheFactoryDelegateBuilder<R> backoffRetryStrategy(long maxAttempts, Duration minBackoff) {
            return retryStrategy(backoff(maxAttempts, minBackoff));
        }

        @Override
        public AutoCacheFactoryDelegateBuilder<R> backoffRetryStrategy(long maxAttempts, Duration minBackoff, Duration maxBackoff) {
            return retryStrategy(backoff(maxAttempts, minBackoff).maxBackoff(maxBackoff));
        }

        @Override
        public AutoCacheFactoryDelegateBuilder<R> fixedDelayRetryStrategy(long maxAttempts, Duration fixedDelay) {
            return retryStrategy(fixedDelay(maxAttempts, fixedDelay));
        }

        @Override
        public AutoCacheFactoryDelegateBuilder<R> retryStrategy(RetrySpec retrySpec) {
            this.cacheTransformer = ConcurrentCacheFactory.concurrent(retrySpec);
            return this;
        }

        @Override
        public AutoCacheFactoryDelegateBuilder<R> retryStrategy(RetryBackoffSpec retryBackoffSpec) {
            this.cacheTransformer = ConcurrentCacheFactory.concurrent(retryBackoffSpec, this.scheduler);
            return this;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <ID, RRC> CacheTransformer<ID, R, RRC> build() {
            return autoCache(dataSource, windowingStrategy, errorHandler, eventSource, scheduler, (CacheTransformer<ID, R, RRC>) cacheTransformer);
        }
    }
}
