package io.github.pellse.reactive.assembler.caching;

import io.github.pellse.reactive.assembler.caching.CacheEvent.Updated;
import io.github.pellse.reactive.assembler.caching.CacheFactory.Context;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.github.pellse.reactive.assembler.caching.AutoCacheFactory.OnErrorStop.onErrorStop;
import static io.github.pellse.reactive.assembler.caching.ConcurrentCache.concurrent;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.partitioningBy;

public interface AutoCacheFactory {

    int MAX_WINDOW_SIZE = 1;

    interface WindowingStrategyBuilder<R, T extends CacheEvent<R>> extends ConfigBuilder<R> {
        ConfigBuilder<R> maxWindowSize(int maxWindowSize);
        ConfigBuilder<R> maxWindowTime(Duration maxWindowTime);
        ConfigBuilder<R> maxWindowSizeAndTime(int maxWindowSize, Duration maxWindowTime);
        ConfigBuilder<R> windowingStrategy(WindowingStrategy<T> windowingStrategy);
    }

    interface ConfigBuilder<R> extends LifeCycleEventSourceBuilder<R> {
        LifeCycleEventSourceBuilder<R> errorHandler(ErrorHandler errorHandler);
    }

    interface LifeCycleEventSourceBuilder<R> extends AutoCacheBuilder<R> {
        AutoCacheBuilder<R> lifeCycleEventSource(LifeCycleEventSource eventSource);
    }

    interface AutoCacheBuilder<R> {
        <ID, RRC> Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> build();
    }

    interface LifeCycleEventSource {
        void addLifeCycleEventListener(LifeCycleEventListener listener);
    }

    interface LifeCycleEventListener {
        void onStart();

        void onComplete();
    }

    class Builder<R, T extends CacheEvent<R>> implements WindowingStrategyBuilder<R, T> {

        private final Flux<T> dataSource;
        private WindowingStrategy<T> windowingStrategy = flux -> flux.window(MAX_WINDOW_SIZE);
        private ErrorHandler errorHandler = onErrorStop();
        private LifeCycleEventSource eventSource = LifeCycleEventListener::onStart;

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
        public AutoCacheBuilder<R> lifeCycleEventSource(LifeCycleEventSource eventSource) {
            this.eventSource = eventSource;
            return this;
        }

        @Override
        public <ID, RRC> Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> build() {
            return autoCache(this.dataSource, this.windowingStrategy, this.errorHandler, this.eventSource);
        }
    }

    sealed interface ErrorHandler {
        <T> Function<Flux<T>, Flux<T>> toFluxErrorHandler();
    }

    record OnErrorContinue(Consumer<Throwable> errorConsumer) implements ErrorHandler {
        public static OnErrorContinue onErrorContinue(Consumer<Throwable> errorConsumer) {
            return new OnErrorContinue(errorConsumer);
        }

        @Override
        public <T> Function<Flux<T>, Flux<T>> toFluxErrorHandler() {
            return flux -> flux.onErrorContinue((error, object) -> errorConsumer().accept(error));
        }
    }

    record OnErrorMap(Function<? super Throwable, ? extends Throwable> mapper) implements ErrorHandler {
        public static OnErrorMap onErrorMap(Function<? super Throwable, ? extends Throwable> mapper) {
            return new OnErrorMap(mapper);
        }

        @Override
        public <T> Function<Flux<T>, Flux<T>> toFluxErrorHandler() {
            return flux -> flux.onErrorMap(mapper());
        }
    }

    record OnErrorStop() implements ErrorHandler {
        public static OnErrorStop onErrorStop() {
            return new OnErrorStop();
        }

        @Override
        public <T> Function<Flux<T>, Flux<T>> toFluxErrorHandler() {
            return Flux::onErrorStop;
        }
    }

    @FunctionalInterface
    interface WindowingStrategy<R> extends Function<Flux<R>, Flux<Flux<R>>> {
    }

    static <R> Flux<CacheEvent<R>> toCacheEvents(Flux<R> flux) {
        return flux.map(Updated::new);
    }

    static <R> WindowingStrategyBuilder<R, CacheEvent<R>> autoCacheFlux(Flux<R> dataSource) {
        return autoCache(toCacheEvents(dataSource));
    }

    static <R, T extends CacheEvent<R>> WindowingStrategyBuilder<R, T> autoCache(Flux<T> dataSource) {
        return new Builder<>(dataSource);
    }

    static <ID, R, RRC, T extends CacheEvent<R>> Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> autoCache(
            Flux<T> dataSource,
            WindowingStrategy<T> windowingStrategy,
            ErrorHandler errorHandler,
            LifeCycleEventSource lifeCycleEventSource) {

        return cacheFactory -> (fetchFunction, context) -> {
            var cache = concurrent(cacheFactory.create(fetchFunction, context));

            var cacheSourceFlux = dataSource.transform(windowingStrategy)
                    .flatMap(flux -> flux.collect(partitioningBy(Updated.class::isInstance)))
                    .flatMap(eventMap -> cache.updateAll(toMap(eventMap.get(true), context), toMap(eventMap.get(false), context)))
                    .transform(errorHandler.toFluxErrorHandler());

            lifeCycleEventSource.addLifeCycleEventListener(new LifeCycleEventListener() {

                private Disposable disposable;

                @Override
                public void onStart() {
                    if (this.disposable ==  null || this.disposable.isDisposed()) {
                        this.disposable = cacheSourceFlux.subscribe();
                    }
                }

                @Override
                public void onComplete() {
                    if (!this.disposable.isDisposed()) {
                        this.disposable.dispose();
                    }
                }
            });

            return cache;
        };
    }

    private static <ID, R, RRC> Map<ID, List<R>> toMap(List<? extends CacheEvent<R>> cacheEvents, Context<ID, R, RRC> context) {
        return cacheEvents.stream()
                .map(CacheEvent::value)
                .collect(groupingBy(context.correlationIdExtractor()));
    }
}
