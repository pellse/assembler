package io.github.pellse.reactive.assembler;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.github.pellse.util.ObjectUtils.also;
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

    Map<ID, List<R>> getAllPresent(Iterable<ID> ids);

    void putAll(Map<ID, List<R>> map);

    static <ID, R> Cache<ID, R> cache() {
        return cache(ConcurrentHashMap::new);
    }

    static <ID, R> Cache<ID, R> cache(Supplier<Map<ID, List<R>>> mapSupplier) {
        return cache(mapSupplier.get());
    }

    static <ID, R> Cache<ID, R> cache(Map<ID, List<R>> delegateMap) {
        return new Cache<>() {

            @Override
            public Map<ID, List<R>> getAllPresent(Iterable<ID> ids) {
                return also(new HashMap<>(), copyMap -> ids.forEach(id -> ifNotNull(delegateMap.get(id), value -> copyMap.put(id, value))));
            }

            @Override
            public void putAll(Map<ID, List<R>> map) {
                delegateMap.putAll(map);
            }
        };
    }

    static <ID, R> RuleMapperSource<ID, List<ID>, R> cached(Function<List<ID>, Publisher<R>> queryFunction) {
        return cached(queryFunction, ConcurrentHashMap::new);
    }

    static <ID, R> RuleMapperSource<ID, List<ID>, R> cached(Function<List<ID>, Publisher<R>> queryFunction, Supplier<Map<ID, List<R>>> mapFactory) {
        return cached(queryFunction, mapFactory.get());
    }

    static <ID, R> RuleMapperSource<ID, List<ID>, R> cached(Function<List<ID>, Publisher<R>> queryFunction, Map<ID, List<R>> map) {
        return cached(queryFunction, cache(map));
    }

    static <ID, R> RuleMapperSource<ID, List<ID>, R> cached(Function<List<ID>, Publisher<R>> queryFunction, Cache<ID, R> cache) {
        return cached(queryFunction, cache::getAllPresent, cache::putAll);
    }

    static <ID, R> RuleMapperSource<ID, List<ID>, R> cached(
            Function<List<ID>, Publisher<R>> queryFunction,
            Function<Iterable<ID>, Map<ID, List<R>>> getAllPresent,
            Consumer<Map<ID, List<R>>> putAll
    ) {
        return cached(queryFunction, ArrayList::new, getAllPresent, putAll);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapperSource<ID, IDC, R> cached(
            Function<IDC, Publisher<R>> queryFunction,
            Supplier<IDC> idCollectionFactory,
            Function<Iterable<ID>, Map<ID, List<R>>> getAllPresent,
            Consumer<Map<ID, List<R>>> putAll
    ) {
        return idExtractor -> ids -> cachedPublisher(ids, idExtractor, queryFunction, idCollectionFactory, getAllPresent, putAll);
    }

    private static <ID, IDC extends Collection<ID>, R> Publisher<R> cachedPublisher(
            IDC ids,
            Function<R, ID> idExtractor,
            Function<IDC, Publisher<R>> queryFunction,
            Supplier<IDC> idCollectionFactory,
            Function<Iterable<ID>, Map<ID, List<R>>> getAllPresent,
            Consumer<Map<ID, List<R>>> putAll
    ) {
        var cachedEntitiesMap = getAllPresent.apply(ids);

        var entityIds = intersect(ids, cachedEntitiesMap.keySet());
        var cachedFlux = fromStream(cachedEntitiesMap.values().stream().flatMap(Collection::stream));

        return entityIds.isEmpty() ? cachedFlux : Flux.merge(
                Flux.from(queryFunction.apply(translate(entityIds, idCollectionFactory)))
                        .collectList()
                        .doOnNext(entities -> putAll.accept(buildMap(entityIds, entities, idExtractor)))
                        .flatMapIterable(identity()),
                cachedFlux);
    }

    private static <ID, R> Map<ID, List<R>> buildMap(Set<ID> entityIds, List<R> entities, Function<R, ID> idExtractor) {
        return also(new HashMap<>(entities.stream().collect(groupingBy(idExtractor, toList()))),
                map -> intersect(entityIds, map.keySet()).forEach(id -> map.put(id, List.of())));
    }
}
