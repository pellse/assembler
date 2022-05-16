package io.github.pellse.reactive.assembler;

import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static io.github.pellse.util.ObjectUtils.then;
import static io.github.pellse.util.collection.CollectionUtil.*;

public interface Cache<ID, R> {
    Mono<Map<ID, Collection<R>>> getAll(Iterable<ID> ids, boolean computeIfAbsent);

    void putAll(Map<ID, Collection<R>> map);

    static <ID, R> CacheFactory<ID, R> cache() {
        return cache(ConcurrentHashMap::new);
    }

    static <ID, R> CacheFactory<ID, R> cache(Supplier<Map<ID, Collection<R>>> mapSupplier) {
        return cache(mapSupplier.get());
    }

    static <ID, R> CacheFactory<ID, R> cache(Map<ID, Collection<R>> delegateMap) {

        return (fetchFunction, idExtractor) -> new Cache<>() {
            @Override
            public Mono<Map<ID, Collection<R>>> getAll(Iterable<ID> ids, boolean computeIfAbsent) {
                return Mono.just(readAll(ids, delegateMap))
                        .flatMap(cachedEntitiesMap -> then(intersect(ids, cachedEntitiesMap.keySet()), entityIds ->
                                !computeIfAbsent || entityIds.isEmpty() ? Mono.just(cachedEntitiesMap) :
                                        fetchFunction.apply(ids)
                                                .doOnNext(delegateMap::putAll)
                                                .map(map -> mergeMaps(map, cachedEntitiesMap))));
            }

            @Override
            public void putAll(Map<ID, Collection<R>> map) {
                delegateMap.putAll(map);
            }
        };
    }
}
