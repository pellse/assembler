package io.github.pellse.reactive.assembler;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.github.pellse.util.ObjectUtils.ifNotNull;
import static io.github.pellse.util.collection.CollectionUtil.intersect;
import static io.github.pellse.util.collection.CollectionUtil.translate;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static reactor.core.publisher.Flux.fromStream;

/**
 * @param <ID> Correlation ID type
 * @param <R>  Entity type
 */
public interface Cache<ID, R> {

    List<R> get(ID id);

    void put(ID id, List<R> entities);

    static <ID, R> RuleMapperSource<ID, List<ID>, R> cached(Function<List<ID>, Publisher<R>> queryFunction) {
        return cached(queryFunction, ConcurrentHashMap::new);
    }

    static <ID, R> RuleMapperSource<ID, List<ID>, R> cached(Function<List<ID>, Publisher<R>> queryFunction,
                                                            Supplier<Map<ID, List<R>>> mapFactory) {
        return cached(queryFunction, mapFactory.get());
    }

    static <ID, R> RuleMapperSource<ID, List<ID>, R> cached(Function<List<ID>, Publisher<R>> queryFunction,
                                                            Map<ID, List<R>> map) {
        return cached(queryFunction, map::get, map::put);
    }

    static <ID, R> RuleMapperSource<ID, List<ID>, R> cached(Function<List<ID>, Publisher<R>> queryFunction,
                                                            Cache<ID, R> cache) {
        return cached(queryFunction, cache::get, cache::put);
    }

    static <ID, R> RuleMapperSource<ID, List<ID>, R> cached(Function<List<ID>, Publisher<R>> queryFunction,
                                                            Function<ID, List<R>> cacheGet,
                                                            BiConsumer<ID, List<R>> cachePut) {
        return cached(queryFunction, ArrayList::new, cacheGet, cachePut);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapperSource<ID, IDC, R> cached(Function<IDC, Publisher<R>> queryFunction,
                                                                                   Supplier<IDC> idCollectionFactory,
                                                                                   Function<ID, List<R>> cacheGet,
                                                                                   BiConsumer<ID, List<R>> cachePut) {
        return idExtractor -> ids -> {

            var cachedEntitiesMap = new HashMap<ID, List<R>>();
            ids.forEach(id -> ifNotNull(cacheGet.apply(id), value -> cachedEntitiesMap.put(id, value)));

            var entityIds = intersect(ids, cachedEntitiesMap.keySet());
            var cachedFlux = fromStream(cachedEntitiesMap.values().stream().flatMap(Collection::stream));

            if (entityIds.isEmpty())
                return cachedFlux;

            var fetchedEntitiesFlux = Flux.from(queryFunction.apply(translate(entityIds, idCollectionFactory)))
                    .collectList()
                    .doOnNext(entities -> cacheEntities(entityIds, entities, idExtractor, cachePut))
                    .flatMapIterable(identity());

            return Flux.merge(fetchedEntitiesFlux, cachedFlux);
        };
    }

    private static <ID, R> void cacheEntities(Set<ID> entityIds, List<R> entities, Function<R, ID> idExtractor, BiConsumer<ID, List<R>> cachePut) {
        var map = entities.stream().collect(groupingBy(idExtractor, toList()));
        map.forEach(cachePut);

        intersect(entityIds, map.keySet())
                .forEach(id -> cachePut.accept(id, List.of()));
    }
}
