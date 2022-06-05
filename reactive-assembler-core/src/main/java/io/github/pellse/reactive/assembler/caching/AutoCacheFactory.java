package io.github.pellse.reactive.assembler.caching;

import io.github.pellse.reactive.assembler.caching.CacheEvent.AddUpdateEvent;
import io.github.pellse.reactive.assembler.caching.CacheFactory.Context;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.github.pellse.reactive.assembler.caching.AutoCacheFactory.OnErrorStop.onErrorStop;
import static io.github.pellse.reactive.assembler.caching.ConcurrentCache.execute;
import static java.util.stream.Collectors.partitioningBy;

public interface AutoCacheFactory {

    sealed interface ErrorHandler {
        Function<Flux<?>, Flux<?>> toFluxErrorHandler();
    }

    record OnErrorContinue(Consumer<Throwable> errorConsumer) implements ErrorHandler {
        public static OnErrorContinue onErrorContinue(Consumer<Throwable> errorConsumer) {
            return new OnErrorContinue(errorConsumer);
        }

        @Override
        public Function<Flux<?>, Flux<?>> toFluxErrorHandler() {
            return flux -> flux.onErrorContinue((error, object) -> errorConsumer().accept(error));
        }
    }

    record OnErrorMap(Function<? super Throwable, ? extends Throwable> mapper) implements ErrorHandler {
        public static OnErrorMap onErrorMap(Function<? super Throwable, ? extends Throwable> mapper) {
            return new OnErrorMap(mapper);
        }

        @Override
        public Function<Flux<?>, Flux<?>> toFluxErrorHandler() {
            return flux -> flux.onErrorMap(mapper());
        }
    }

    record OnErrorStop() implements ErrorHandler {
        public static OnErrorStop onErrorStop() {
            return new OnErrorStop();
        }

        @Override
        public Function<Flux<?>, Flux<?>> toFluxErrorHandler() {
            return Flux::onErrorStop;
        }
    }

    int MAX_WINDOW_SIZE = 1;

    interface CRUDCache<ID, RRC> extends Cache<ID, RRC> {

        Mono<?> updateAll(Map<ID, RRC> mapMonoToAdd, Map<ID, RRC> mapMonoToRemove);
    }

    @FunctionalInterface
    interface WindowingStrategy<R> {
        Flux<Flux<R>> toWindowedFlux(Flux<R> flux);
    }

    static <R> Flux<CacheEvent<R>> toCacheEvent(Flux<R> flux) {
        return flux.map(AddUpdateEvent::new);
    }

    static <ID, R, RRC> Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> autoCache(
            Flux<? extends CacheEvent<R>> dataSource) {
        return autoCache(dataSource, MAX_WINDOW_SIZE);
    }

    static <ID, R, RRC> Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> autoCache(
            Flux<? extends CacheEvent<R>> dataSource,
            ErrorHandler handler) {
        return autoCache(dataSource, MAX_WINDOW_SIZE, handler);
    }

    static <ID, R, RRC> Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> autoCache(
            Flux<? extends CacheEvent<R>> dataSource,
            int maxWindowSize) {
        return autoCache(dataSource, flux -> flux.window(maxWindowSize));
    }

    static <ID, R, RRC> Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> autoCache(
            Flux<? extends CacheEvent<R>> dataSource,
            int maxWindowSize,
            ErrorHandler handler) {
        return autoCache(dataSource, flux -> flux.window(maxWindowSize), handler);
    }

    static <ID, R, RRC> Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> autoCache(
            Flux<? extends CacheEvent<R>> dataSource,
            Duration maxWindowTime) {
        return autoCache(dataSource, flux -> flux.window(maxWindowTime));
    }

    static <ID, R, RRC> Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> autoCache(
            Flux<? extends CacheEvent<R>> dataSource,
            Duration maxWindowTime,
            ErrorHandler handler) {
        return autoCache(dataSource, flux -> flux.window(maxWindowTime), handler);
    }

    static <ID, R, RRC> Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> autoCache(
            Flux<? extends CacheEvent<R>> dataSource,
            int maxWindowSize,
            Duration maxWindowTime) {
        return autoCache(dataSource, flux -> flux.windowTimeout(maxWindowSize, maxWindowTime));
    }

    static <ID, R, RRC> Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> autoCache(
            Flux<? extends CacheEvent<R>> dataSource,
            int maxWindowSize,
            Duration maxWindowTime,
            ErrorHandler handler) {
        return autoCache(dataSource, flux -> flux.windowTimeout(maxWindowSize, maxWindowTime), handler);
    }

    static <ID, R, RRC, T extends CacheEvent<R>> Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> autoCache(
            Flux<T> dataSource,
            WindowingStrategy<T> windowingStrategy) {

        return autoCache(dataSource, windowingStrategy, onErrorStop());
    }

    static <ID, R, RRC, T extends CacheEvent<R>> Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> autoCache(
            Flux<T> dataSource,
            WindowingStrategy<T> windowingStrategy,
            ErrorHandler errorHandler) {

        return cacheFactory -> (fetchFunction, context) -> {
            var cache = synchronous(cacheFactory.create(fetchFunction, context));

            var cacheSourceFlux = errorHandler.toFluxErrorHandler().apply(
                    windowingStrategy.toWindowedFlux(dataSource)
                            .flatMap(flux -> flux.collect(partitioningBy(value -> value instanceof AddUpdateEvent)))
                            .flatMap(eventMap -> cache.updateAll(toMap(eventMap.get(true), context), toMap(eventMap.get(false), context))));

            cacheSourceFlux.subscribe();
            return cache;
        };
    }

    private static <ID, RRC> CRUDCache<ID, RRC> synchronous(Cache<ID, RRC> delegateCache) {

        return new CRUDCache<>() {

            private final AtomicBoolean shouldRunFlag = new AtomicBoolean();

            @Override
            public Mono<Map<ID, RRC>> getAll(Iterable<ID> ids, boolean computeIfAbsent) {
                return execute(delegateCache.getAll(ids, computeIfAbsent), shouldRunFlag);
            }

            @Override
            public Mono<?> putAll(Map<ID, RRC> map) {
                return execute(delegateCache.putAll(map), shouldRunFlag);
            }

            @Override
            public Mono<?> removeAll(Map<ID, RRC> map) {
                return execute(delegateCache.removeAll(map), shouldRunFlag);
            }

            @Override
            public Mono<?> updateAll(Map<ID, RRC> mapMonoToAdd, Map<ID, RRC> mapMonoToRemove) {
                return execute(
                        delegateCache.putAll(mapMonoToAdd).then(delegateCache.removeAll(mapMonoToRemove)),
                        shouldRunFlag);
            }
        };
    }

    private static <ID, R, RRC> Map<ID, RRC> toMap(List<? extends CacheEvent<R>> cacheEvents, Context<ID, R, RRC> context) {
        return cacheEvents.stream()
                .map(CacheEvent::value)
                .collect(context.mapCollector().apply(-1));
    }
}
