package io.github.pellse.reactive.assembler.caching;

import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static io.github.pellse.util.ObjectUtils.also;
import static io.github.pellse.util.ObjectUtils.then;
import static io.github.pellse.util.collection.CollectionUtil.*;
import static java.util.Collections.unmodifiableMap;
import static reactor.core.publisher.Mono.just;

public interface Cache<ID, RRC> {
    Mono<Map<ID, RRC>> getAll(Iterable<ID> ids, boolean computeIfAbsent);

    Mono<Map<ID, RRC>> putAll(Mono<Map<ID, RRC>> mapMono);

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
            public Mono<Map<ID, RRC>> putAll(Mono<Map<ID, RRC>> mapMono) {
                return mapMono.map(m -> also(m, delegateMap::putAll));
            }
        };
    }

    static <ID, RRC> Cache<ID, RRC> delegateCache(
            Cache<ID, RRC> delegateCache,
            MergeStrategy<ID, RRC> mergeStrategy) {

        return new Cache<>() {

            @Override
            public Mono<Map<ID, RRC>> getAll(Iterable<ID> ids, boolean computeIfAbsent) {
                return delegateCache.getAll(ids, computeIfAbsent);
            }

            @Override
            public Mono<Map<ID, RRC>> putAll(Mono<Map<ID, RRC>> mapMono) {
                return mapMono
                        .map(map -> delegateCache.getAll(map.keySet(), false)
                                .map(mapFromCache -> mergeStrategy.merge(new HashMap<>(mapFromCache), unmodifiableMap(map))))
                        .flatMap(delegateCache::putAll);
            }
        };
    }
}
