package io.github.pellse.reactive.assembler.caching;

import io.github.pellse.reactive.assembler.LifeCycleEventSource;
import io.github.pellse.reactive.assembler.LifeCycleEventSource.LifeCycleEventListener;
import io.github.pellse.reactive.assembler.caching.AutoCacheFactory.AutoCacheFactoryDelegate;
import io.github.pellse.reactive.assembler.caching.AutoCacheFactory.ErrorHandler;
import io.github.pellse.reactive.assembler.caching.AutoCacheFactory.WindowingStrategy;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.function.Function;

import static io.github.pellse.reactive.assembler.caching.AutoCacheFactory.MAX_WINDOW_SIZE;
import static io.github.pellse.reactive.assembler.caching.AutoCacheFactory.OnErrorStop.onErrorStop;

public interface AutoCacheFactoryBuilder {

    interface WindowingStrategyBuilder<R, T extends CacheEvent<R>> extends ConfigBuilder<R> {
        ConfigBuilder<R> maxWindowSize(int maxWindowSize);

        ConfigBuilder<R> maxWindowTime(Duration maxWindowTime);

        ConfigBuilder<R> maxWindowSizeAndTime(int maxWindowSize, Duration maxWindowTime);

        ConfigBuilder<R> windowingStrategy(WindowingStrategy<T> windowingStrategy);
    }

    interface ConfigBuilder<R> extends LifeCycleEventSourceBuilder<R> {
        LifeCycleEventSourceBuilder<R> errorHandler(ErrorHandler errorHandler);
    }

    interface LifeCycleEventSourceBuilder<R> extends AutoCacheFactoryDelegateBuilder<R> {
        AutoCacheFactoryDelegateBuilder<R> lifeCycleEventSource(LifeCycleEventSource eventSource);
    }

    interface AutoCacheFactoryDelegateBuilder<R> {
        <ID, RRC> AutoCacheFactoryDelegate<ID, R, RRC> build();
    }

    class Builder<R, T extends CacheEvent<R>> implements WindowingStrategyBuilder<R, T> {

        private final Flux<T> dataSource;
        private WindowingStrategy<T> windowingStrategy = flux -> flux.window(MAX_WINDOW_SIZE);
        private ErrorHandler errorHandler = onErrorStop();

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
        public AutoCacheFactoryDelegateBuilder<R> lifeCycleEventSource(LifeCycleEventSource eventSource) {
            this.eventSource = eventSource;
            return this;
        }

        @Override
        public <ID, RRC> AutoCacheFactoryDelegate<ID, R, RRC> build() {
            return AutoCacheFactory.autoCache(dataSource, windowingStrategy, errorHandler, eventSource);
        }
    }

    static <R> WindowingStrategyBuilder<R, CacheEvent<R>> autoCache(Flux<R> dataSource) {
        return autoCache(dataSource, CacheEvent::updated);
    }

    static <R, T extends CacheEvent<R>> WindowingStrategyBuilder<R, T> autoCache(Flux<R> dataSource, Function<R, T> mapper) {
        return autoCacheEvents(dataSource.map(mapper));
    }

    static <R, T extends CacheEvent<R>> WindowingStrategyBuilder<R, T> autoCacheEvents(Flux<T> dataSource) {
        return new Builder<>(dataSource);
    }
}
