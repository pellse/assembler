package io.github.pellse.reactive.assembler;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;

import static io.github.pellse.reactive.assembler.Cache.cache;
import static io.github.pellse.util.collection.CollectionUtil.*;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Stream.concat;

public interface AutoCacheFactory {

    int MAX_WINDOW_SIZE = 1;

    @FunctionalInterface
    interface WindowingStrategy<R> {
        Flux<Flux<R>> toWindowedFlux(Flux<R> flux);
    }

    static <ID, R> CacheFactory<ID, R> autoCache(Flux<R> dataSource) {
        return autoCache(dataSource, cache());
    }

    static <ID, R> CacheFactory<ID, R> autoCache(Flux<R> dataSource, CacheFactory<ID, R> cacheFactory) {
        return autoCache(dataSource, MAX_WINDOW_SIZE, cacheFactory);
    }

    static <ID, R> CacheFactory<ID, R> autoCache(Flux<R> dataSource, int maxWindowSize, CacheFactory<ID, R> cacheFactory) {
        return autoCache(dataSource, flux -> flux.window(maxWindowSize), cacheFactory);
    }

    static <ID, R> CacheFactory<ID, R> autoCache(Flux<R> dataSource, Duration maxWindowTime, CacheFactory<ID, R> cacheFactory) {
        return autoCache(dataSource, flux -> flux.window(maxWindowTime), cacheFactory);
    }

    static <ID, R> CacheFactory<ID, R> autoCache(Flux<R> dataSource, int maxWindowSize, Duration maxWindowTime, CacheFactory<ID, R> cacheFactory) {
        return autoCache(dataSource, flux -> flux.windowTimeout(maxWindowSize, maxWindowTime), cacheFactory);
    }

    static <ID, R> CacheFactory<ID, R> autoCache(Flux<R> dataSource, WindowingStrategy<R> windowingStrategy, CacheFactory<ID, R> cacheFactory) {
        return (fetchFunction, context) -> {
            var cache = cacheFactory.create(fetchFunction, context);

            var cacheSourceFlux = windowingStrategy.toWindowedFlux(dataSource)
                    .flatMap(flux -> flux.collectMultimap(context.idExtractor()))
                    .flatMap(map -> mergeWithCache(map, cache))
                    .doOnNext(cache::putAll);

            cacheSourceFlux.subscribe();
            return cache;
        };
    }

    private static <ID, R> Mono<Map<ID, Collection<R>>> mergeWithCache(Map<ID, Collection<R>> map, Cache<ID, R> cache) {
        var keys = map.keySet();
        return cache.getAll(keys, false)
                .map(subMapFromCache ->
                        newMap(subMapFromCache, m ->
                                m.replaceAll((id, coll) ->
                                        concat(toStream(coll), toStream(map.get(id)))
                                                .collect(toCollection(LinkedHashSet::new)))))
                .map(partialMap -> mergeMaps(partialMap, readAll(intersect(keys, partialMap.keySet()), map)));
    }
}
