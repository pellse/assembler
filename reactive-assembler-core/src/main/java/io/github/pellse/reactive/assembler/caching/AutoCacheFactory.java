package io.github.pellse.reactive.assembler.caching;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static io.github.pellse.reactive.assembler.caching.Cache.cache;
import static io.github.pellse.reactive.assembler.caching.MergeStrategy.append;
import static java.util.Collections.unmodifiableMap;

public interface AutoCacheFactory {

    int MAX_WINDOW_SIZE = 1;

    @FunctionalInterface
    interface WindowingStrategy<R> {
        Flux<Flux<R>> toWindowedFlux(Flux<R> flux);
    }

    static <ID, R> CacheFactory<ID, R> autoCache(Flux<R> dataSource) {
        return autoCache(dataSource, cache());
    }

    static <ID, R> CacheFactory<ID, R> autoCache(Flux<R> dataSource, MergeStrategy<ID, R> mergeStrategy) {
        return autoCache(dataSource, mergeStrategy, cache());
    }

    static <ID, R> CacheFactory<ID, R> autoCache(Flux<R> dataSource, CacheFactory<ID, R> cacheFactory) {
        return autoCache(dataSource, MAX_WINDOW_SIZE, cacheFactory);
    }

    static <ID, R> CacheFactory<ID, R> autoCache(Flux<R> dataSource, MergeStrategy<ID, R> mergeStrategy, CacheFactory<ID, R> cacheFactory) {
        return autoCache(dataSource, MAX_WINDOW_SIZE, mergeStrategy, cacheFactory);
    }

    static <ID, R> CacheFactory<ID, R> autoCache(Flux<R> dataSource, int maxWindowSize, CacheFactory<ID, R> cacheFactory) {
        return autoCache(dataSource, flux -> flux.window(maxWindowSize), cacheFactory);
    }

    static <ID, R> CacheFactory<ID, R> autoCache(Flux<R> dataSource, int maxWindowSize, MergeStrategy<ID, R> mergeStrategy, CacheFactory<ID, R> cacheFactory) {
        return autoCache(dataSource, flux -> flux.window(maxWindowSize), mergeStrategy, cacheFactory);
    }

    static <ID, R> CacheFactory<ID, R> autoCache(Flux<R> dataSource, Duration maxWindowTime, CacheFactory<ID, R> cacheFactory) {
        return autoCache(dataSource, flux -> flux.window(maxWindowTime), cacheFactory);
    }

    static <ID, R> CacheFactory<ID, R> autoCache(Flux<R> dataSource, Duration maxWindowTime, MergeStrategy<ID, R> mergeStrategy, CacheFactory<ID, R> cacheFactory) {
        return autoCache(dataSource, flux -> flux.window(maxWindowTime), mergeStrategy, cacheFactory);
    }

    static <ID, R> CacheFactory<ID, R> autoCache(
            Flux<R> dataSource,
            int maxWindowSize,
            Duration maxWindowTime,
            CacheFactory<ID, R> cacheFactory) {
        return autoCache(dataSource, flux -> flux.windowTimeout(maxWindowSize, maxWindowTime), cacheFactory);
    }

    static <ID, R> CacheFactory<ID, R> autoCache(
            Flux<R> dataSource,
            int maxWindowSize,
            Duration maxWindowTime,
            MergeStrategy<ID, R> mergeStrategy,
            CacheFactory<ID, R> cacheFactory) {
        return autoCache(dataSource, flux -> flux.windowTimeout(maxWindowSize, maxWindowTime), mergeStrategy, cacheFactory);
    }

    static <ID, R> CacheFactory<ID, R> autoCache(
            Flux<R> dataSource,
            WindowingStrategy<R> windowingStrategy,
            CacheFactory<ID, R> cacheFactory) {

        return autoCache(dataSource, windowingStrategy, append(), cacheFactory);
    }

    static <ID, R> CacheFactory<ID, R> autoCache(
            Flux<R> dataSource,
            WindowingStrategy<R> windowingStrategy,
            MergeStrategy<ID, R> mergeStrategy,
            CacheFactory<ID, R> cacheFactory) {

        return (fetchFunction, context) -> {
            var cache = cacheFactory.create(fetchFunction, context);

            var cacheSourceFlux = windowingStrategy.toWindowedFlux(dataSource)
                    .flatMap(flux -> flux.collectMultimap(context.idExtractor()))
                    .flatMap(map -> mergeWithCache(mergeStrategy, map, cache))
                    .doOnNext(cache::putAll);

            cacheSourceFlux.subscribe();
            return cache;
        };
    }

    private static <ID, R> Mono<Map<ID, Collection<R>>> mergeWithCache(
            MergeStrategy<ID, R> mergeStrategy,
            Map<ID, Collection<R>> map,
            Cache<ID, R> cache) {
        return cache.getAll(map.keySet(), false)
                .map(subMapFromCache -> mergeStrategy.merge(new HashMap<>(subMapFromCache), unmodifiableMap(map)));
    }
}
