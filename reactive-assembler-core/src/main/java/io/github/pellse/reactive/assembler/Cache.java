package io.github.pellse.reactive.assembler;

import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static io.github.pellse.util.ObjectUtils.ifNotNull;
import static io.github.pellse.util.ObjectUtils.then;
import static io.github.pellse.util.collection.CollectionUtil.*;

@FunctionalInterface
public interface Cache<ID, R> {
    Mono<Map<ID, Collection<R>>> getAll(Iterable<ID> ids);

    static <ID, R> CacheFactory<ID, R> cache() {
        return cache(ConcurrentHashMap::new);
    }

    static <ID, R> CacheFactory<ID, R> cache(Supplier<Map<ID, Collection<R>>> mapSupplier) {
        return cache(mapSupplier.get());
    }

    static <ID, R> CacheFactory<ID, R> cache(Map<ID, Collection<R>> delegateMap) {
        return fetchFunction -> ids -> Mono.just(getAll(ids, delegateMap))
                .flatMap(cachedEntitiesMap -> then(intersect(ids, cachedEntitiesMap.keySet()), entityIds ->
                        entityIds.isEmpty() ? Mono.just(cachedEntitiesMap) :
                                fetchFunction.apply(ids)
                                        .doOnNext(delegateMap::putAll)
                                        .map(map -> mergeMaps(map, cachedEntitiesMap))));
    }

    private static <ID, R> Map<ID, Collection<R>> getAll(Iterable<ID> ids, Map<ID, Collection<R>> sourceMap) {
        return newMap(map -> ids.forEach(id -> ifNotNull(sourceMap.get(id), value -> map.put(id, value))));
    }
}
