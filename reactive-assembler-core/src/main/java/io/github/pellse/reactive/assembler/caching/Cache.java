package io.github.pellse.reactive.assembler.caching;

import io.github.pellse.util.function.Function3;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.github.pellse.reactive.assembler.caching.AdapterCache.adapterCache;
import static io.github.pellse.reactive.assembler.caching.CacheFactory.toMono;
import static io.github.pellse.util.ObjectUtils.then;
import static io.github.pellse.util.collection.CollectionUtil.*;
import static java.util.Map.of;
import static reactor.core.publisher.Mono.just;

public interface Cache<ID, RRC> {
    Mono<Map<ID, RRC>> getAll(Iterable<ID> ids, boolean computeIfAbsent);

    Mono<?> putAll(Map<ID, RRC> map);

    Mono<?> removeAll(Map<ID, RRC> map);

    static <ID, R, RRC> CacheFactory<ID, R, RRC> cache() {
        return cache(ConcurrentHashMap::new);
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> cache(Supplier<Map<ID, RRC>> mapSupplier) {
        return cache(mapSupplier.get());
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> cache(Map<ID, RRC> delegateMap) {

        return (fetchFunction, $) -> adapterCache(
                (ids, computeIfAbsent) -> just(readAll(ids, delegateMap))
                        .flatMap(cachedEntitiesMap -> then(intersect(ids, cachedEntitiesMap.keySet()), entityIds ->
                                !computeIfAbsent || entityIds.isEmpty() ? just(cachedEntitiesMap) :
                                        fetchFunction.apply(entityIds)
                                                .doOnNext(delegateMap::putAll)
                                                .map(map -> mergeMaps(map, cachedEntitiesMap)))),
                toMono(delegateMap::putAll),
                toMono(map -> delegateMap.keySet().removeAll(map.keySet()))
        );
    }

    static <ID, RRC> Cache<ID, RRC> mergeStrategyAwareCache(
            Cache<ID, RRC> delegateCache,
            MergeStrategy<ID, RRC> mergeStrategy,
            MergeStrategy<ID, RRC> removeStrategy) {

        Cache<ID, RRC> optimizedCache = adapterCache(
                emptyOr(delegateCache::getAll),
                emptyOr(delegateCache::putAll),
                emptyOr(delegateCache::removeAll)
        );

        return adapterCache(
                optimizedCache::getAll,
                applyMergeStrategy(optimizedCache, mergeStrategy, Cache::putAll),
                applyMergeStrategy(optimizedCache, (cache, mapFromCache, newChanges) -> {
                    var mapAfterRemove = removeStrategy.apply(mapFromCache, newChanges);
                    var removedItems = readAll(
                            intersect(mapFromCache.keySet(), mapAfterRemove.keySet()),
                            mapFromCache);

                    return cache.putAll(mapAfterRemove)
                            .then(cache.removeAll(removedItems));
                })
        );
    }

    private static <ID, RRC> Function<Map<ID, RRC>, Mono<?>> applyMergeStrategy(
            Cache<ID, RRC> delegateCache,
            MergeStrategy<ID, RRC> mergeStrategy,
            BiFunction<Cache<ID, RRC>, Map<ID, RRC>, Mono<?>> strategy) {

        return applyMergeStrategy(
                delegateCache,
                (cache, mapFromCache, newChanges) -> strategy.apply(cache, mergeStrategy.apply(mapFromCache, newChanges)));
    }

    private static <ID, RRC> Function<Map<ID, RRC>, Mono<?>> applyMergeStrategy(
            Cache<ID, RRC> delegateCache,
            Function3<Cache<ID, RRC>, Map<ID, RRC>, Map<ID, RRC>, Mono<?>> strategy) {

        return map -> isEmpty(map) ? just(of()) : just(map)
                .flatMap(m -> delegateCache.getAll(m.keySet(), false)
                        .flatMap(mapFromCache -> strategy.apply(delegateCache, mapFromCache, m)));
    }

    private static <ID, RRC> Function<Map<ID, RRC>, Mono<?>> emptyOr(Function<Map<ID, RRC>, Mono<?>> mappingFunction) {
        return map -> isEmpty(map) ? just(of()) : mappingFunction.apply(map);
    }

    private static <ID, RRC> BiFunction<Iterable<ID>, Boolean, Mono<Map<ID, RRC>>> emptyOr(
            BiFunction<Iterable<ID>, Boolean, Mono<Map<ID, RRC>>> mappingFunction) {
        return (ids, computeIfAbsent) -> isEmpty(ids) ? just(of()) : mappingFunction.apply(ids, computeIfAbsent);
    }
}
