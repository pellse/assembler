package io.github.pellse.reactive.assembler.caching;

import io.github.pellse.reactive.assembler.LifeCycleEventSource;
import io.github.pellse.reactive.assembler.caching.CacheEvent.Updated;
import io.github.pellse.reactive.assembler.caching.CacheFactory.Context;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.github.pellse.reactive.assembler.LifeCycleEventSource.lifeCycleEventAdapter;
import static io.github.pellse.reactive.assembler.caching.ConcurrentCache.concurrent;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.partitioningBy;

public interface AutoCacheFactory {

    int MAX_WINDOW_SIZE = 1;

    interface AutoCacheFactoryDelegate<ID, R, RRC> extends Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> {
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

    static <ID, R, RRC, T extends CacheEvent<R>> AutoCacheFactoryDelegate<ID, R, RRC> autoCache(
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

            lifeCycleEventSource.addLifeCycleEventListener(lifeCycleEventAdapter(cacheSourceFlux, Flux::subscribe, Disposable::dispose));

            return cache;
        };
    }

    private static <ID, R, RRC> Map<ID, List<R>> toMap(List<? extends CacheEvent<R>> cacheEvents, Context<ID, R, RRC> context) {
        return cacheEvents.stream()
                .map(CacheEvent::value)
                .collect(groupingBy(context.correlationIdExtractor()));
    }
}
