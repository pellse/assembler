package io.github.pellse.reactive.assembler.caching;

import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static io.github.pellse.reactive.assembler.caching.AdapterCache.adapterCache;
import static io.github.pellse.util.ObjectUtils.also;
import static io.github.pellse.util.ObjectUtils.then;
import static io.github.pellse.util.collection.CollectionUtil.*;
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
                                        fetchFunction.apply(ids)
                                                .doOnNext(delegateMap::putAll)
                                                .map(map -> mergeMaps(map, cachedEntitiesMap)))),
                map -> just(also(map, delegateMap::putAll)),
                map -> just(also(map, m ->  delegateMap.keySet().removeAll(m.keySet())))
        );
    }

    static <ID, RRC> Cache<ID, RRC> mergeStrategyAwareCache(Cache<ID, RRC> delegateCache, MergeStrategy<ID, RRC> mergeStrategy) {

        return adapterCache(
                delegateCache::getAll,
                map -> just(map)
                        .flatMap(m -> delegateCache.getAll(m.keySet(), false)
                                .map(mapFromCache -> mergeStrategy.merge(mapFromCache, m)))
                        .flatMap(delegateCache::putAll),
                delegateCache::removeAll
        );
    }
}
