package io.github.pellse.reactive.assembler.caching;

import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

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

        return (fetchFunction, $) -> new Cache<>() {
            @Override
            public Mono<Map<ID, RRC>> getAll(Iterable<ID> ids, boolean computeIfAbsent) {
                return just(readAll(ids, delegateMap))
                        .flatMap(cachedEntitiesMap -> then(intersect(ids, cachedEntitiesMap.keySet()), entityIds ->
                                !computeIfAbsent || entityIds.isEmpty() ? just(cachedEntitiesMap) :
                                        fetchFunction.apply(ids)
                                                .doOnNext(delegateMap::putAll)
                                                .map(map -> mergeMaps(map, cachedEntitiesMap))));
            }

            @Override
            public Mono<?> putAll(Map<ID, RRC> map) {
                return just(also(map, delegateMap::putAll));
            }

            @Override
            public Mono<?> removeAll(Map<ID, RRC> map) {
                return just(also(map, m ->  delegateMap.keySet().removeAll(m.keySet())));
            }
        };
    }

    static <ID, RRC> Cache<ID, RRC> mergeStrategyAwareCache(Cache<ID, RRC> delegateCache, MergeStrategy<ID, RRC> mergeStrategy) {

        return new Cache<>() {

            @Override
            public Mono<Map<ID, RRC>> getAll(Iterable<ID> ids, boolean computeIfAbsent) {
                return delegateCache.getAll(ids, computeIfAbsent);
            }

            @Override
            public Mono<?> putAll(Map<ID, RRC> map) {
                return just(map)
                        .flatMap(m -> delegateCache.getAll(m.keySet(), false)
                                .map(mapFromCache -> mergeStrategy.merge(mapFromCache, m)))
                        .flatMap(delegateCache::putAll);
            }

            @Override
            public Mono<?> removeAll(Map<ID, RRC> map) {
                return delegateCache.removeAll(map);
            }
        };
    }
}
