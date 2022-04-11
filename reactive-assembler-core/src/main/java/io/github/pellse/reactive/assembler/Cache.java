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

    static <ID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, IDC, R, RRC> cached(Function<IDC, Publisher<R>> queryFunction) {
        return cached(queryFunction, ConcurrentHashMap::new);
    }

    static <ID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, IDC, R, RRC> cached(
            Function<IDC, Publisher<R>> queryFunction,
            Supplier<Map<ID, List<R>>> mapFactory) {
        return cached(queryFunction, mapFactory.get());
    }

    static <ID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, IDC, R, RRC> cached(
            Function<IDC, Publisher<R>> queryFunction,
            Map<ID, List<R>> map) {
        return cached(queryFunction, cache(map));
    }

    static <ID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, IDC, R, RRC> cached(
            Function<IDC, Publisher<R>> queryFunction,
            Cache<ID, R> cache) {
        return cached(queryFunction, cache::getAllPresent, cache::putAll);
    }

    static <ID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, IDC, R, RRC> cached(
            Function<IDC, Publisher<R>> queryFunction,
            Function<Iterable<ID>, Map<ID, List<R>>> getAllPresent,
            Consumer<Map<ID, List<R>>> putAll) {
        return ruleContext -> ids -> cachedPublisher(ids, ruleContext, queryFunction, getAllPresent, putAll);
    }

    private static <ID, IDC extends Collection<ID>, R, RRC> Publisher<R> cachedPublisher(
            IDC ids,
            RuleContext<ID, IDC, R, RRC> ruleContext,
            Function<IDC, Publisher<R>> queryFunction,
            Function<Iterable<ID>, Map<ID, List<R>>> getAllPresent,
            Consumer<Map<ID, List<R>>> putAll) {

        var cachedEntitiesMap = getAllPresent.apply(ids);

        var entityIds = intersect(ids, cachedEntitiesMap.keySet());
        var cachedFlux = fromStream(cachedEntitiesMap.values().stream().flatMap(Collection::stream));

        return entityIds.isEmpty() ? cachedFlux : Flux.merge(
                Flux.from(queryFunction.apply(translate(entityIds, ruleContext.idCollectionFactory())))
                        .collectList()
                        .doOnNext(entities -> putAll.accept(buildCacheFragment(entityIds, entities, ruleContext.idExtractor())))
                        .flatMapIterable(identity()),
                cachedFlux);
    }

    private static <ID, R> Map<ID, List<R>> buildCacheFragment(Set<ID> entityIds, List<R> entities, Function<R, ID> idExtractor) {
        return also(new HashMap<>(entities.stream().collect(groupingBy(idExtractor, toList()))),
                cacheFragment -> intersect(entityIds, cacheFragment.keySet())
                        .forEach(id -> cacheFragment.put(id, List.of())));
    }
}
