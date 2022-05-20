package io.github.pellse.reactive.assembler.caching;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static io.github.pellse.reactive.assembler.caching.Cache.cache;
import static java.util.Collections.unmodifiableMap;

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
            var cache = cacheFactory.create(fetchFunction, context);

            var cacheSourceFlux = windowingStrategy.toWindowedFlux(dataSource)
                    .flatMap(flux -> flux.collect(context.mapCollector().apply(0)))
                    .flatMap(map -> mergeWithCache(context.mergeStrategy(), map, cache))
                    .doOnNext(cache::putAll);

            cacheSourceFlux.subscribe();
            return cache;
        };
    }

    private static <ID, RRC> Mono<Map<ID, RRC>> mergeWithCache(
            MergeStrategy<ID, RRC> mergeStrategy,
            Map<ID, RRC> map,
            Cache<ID, RRC> cache) {
        return cache.getAll(map.keySet(), false)
                .map(subMapFromCache -> mergeStrategy.merge(new HashMap<>(subMapFromCache), unmodifiableMap(map)));
    }
}
