package io.github.pellse.reactive.assembler.caching;

import reactor.core.publisher.Flux;

import java.time.Duration;

import static io.github.pellse.reactive.assembler.caching.Cache.*;
import static io.github.pellse.reactive.assembler.caching.ConcurrentCache.concurrent;

public interface AutoCacheFactory {

    int MAX_WINDOW_SIZE = 1;

    @FunctionalInterface
    interface WindowingStrategy<R> {
        Flux<Flux<R>> toWindowedFlux(Flux<R> flux);
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> autoCache(Flux<R> dataSource) {
        return autoCache(dataSource, cache());
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> autoCache(Flux<R> dataSource, CacheFactory<ID, R, RRC> cacheFactory) {
        return autoCache(dataSource, MAX_WINDOW_SIZE, cacheFactory);
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> autoCache(Flux<R> dataSource, int maxWindowSize) {
        return autoCache(dataSource, maxWindowSize, cache());
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> autoCache(Flux<R> dataSource, int maxWindowSize, CacheFactory<ID, R, RRC> cacheFactory) {
        return autoCache(dataSource, flux -> flux.window(maxWindowSize), cacheFactory);
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> autoCache(Flux<R> dataSource, Duration maxWindowTime) {
        return autoCache(dataSource, maxWindowTime, cache());
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> autoCache(Flux<R> dataSource, Duration maxWindowTime, CacheFactory<ID, R, RRC> cacheFactory) {
        return autoCache(dataSource, flux -> flux.window(maxWindowTime), cacheFactory);
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> autoCache(
            Flux<R> dataSource,
            int maxWindowSize,
            Duration maxWindowTime) {
        return autoCache(dataSource, maxWindowSize, maxWindowTime, cache());
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> autoCache(
            Flux<R> dataSource,
            int maxWindowSize,
            Duration maxWindowTime,
            CacheFactory<ID, R, RRC> cacheFactory) {
        return autoCache(dataSource, flux -> flux.windowTimeout(maxWindowSize, maxWindowTime), cacheFactory);
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> autoCache(Flux<R> dataSource, WindowingStrategy<R> windowingStrategy) {
        return autoCache(dataSource, windowingStrategy, cache());
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> autoCache(
            Flux<R> dataSource,
            WindowingStrategy<R> windowingStrategy,
            CacheFactory<ID, R, RRC> cacheFactory) {

        return (fetchFunction, context) -> {
            var cache = concurrent(delegateCache(cacheFactory.create(fetchFunction, context), context.mergeStrategy()));

            var cacheSourceFlux = windowingStrategy.toWindowedFlux(dataSource)
                    .map(flux -> flux.collect(context.mapCollector().apply(-1)))
                    .flatMap(cache::putAll);

            cacheSourceFlux.subscribe();
            return cache;
        };
    }
}
