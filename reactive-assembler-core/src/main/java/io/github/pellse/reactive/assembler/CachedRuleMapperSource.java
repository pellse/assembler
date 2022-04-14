package io.github.pellse.reactive.assembler;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.github.pellse.reactive.assembler.RuleMapperSource.call;
import static io.github.pellse.util.ObjectUtils.*;
import static io.github.pellse.util.collection.CollectionUtil.*;
import static reactor.core.publisher.Flux.fromStream;

@FunctionalInterface
public interface CachedRuleMapperSource<ID, IDC extends Collection<ID>, R, RRC> extends RuleMapperSource<ID, IDC, R, RRC> {

    Function<Iterable<ID>, Mono<Map<ID, Collection<R>>>> getAll(RuleContext<ID, IDC, R, RRC> ruleContext);

    @Override
    default Function<IDC, Publisher<R>> apply(RuleContext<ID, IDC, R, RRC> ruleContext) {
        return then(getAll(ruleContext), queryFunction -> ids -> queryFunction.apply(ids)
                .flatMapMany(map -> fromStream(map.values().stream().flatMap(Collection::stream))));
    }

    static <ID, IDC extends Collection<ID>, R, RRC> CachedRuleMapperSource<ID, IDC, R, RRC> cached(Function<IDC, Publisher<R>> queryFunction) {
        return cached(call(queryFunction));
    }

    static <ID, IDC extends Collection<ID>, R, RRC> CachedRuleMapperSource<ID, IDC, R, RRC> cached(RuleMapperSource<ID, IDC, R, RRC> ruleMapperSource) {
        return cached(ruleMapperSource, ConcurrentHashMap::new);
    }

    static <ID, IDC extends Collection<ID>, R, RRC> CachedRuleMapperSource<ID, IDC, R, RRC> cached(
            Function<IDC, Publisher<R>> queryFunction,
            Supplier<Map<ID, Collection<R>>> mapSupplier) {
        return cached(call(queryFunction), mapSupplier);
    }

    static <ID, IDC extends Collection<ID>, R, RRC> CachedRuleMapperSource<ID, IDC, R, RRC> cached(
            RuleMapperSource<ID, IDC, R, RRC> ruleMapperSource,
            Supplier<Map<ID, Collection<R>>> mapSupplier) {
        return cached(ruleMapperSource, mapSupplier.get());
    }

    static <ID, IDC extends Collection<ID>, R, RRC> CachedRuleMapperSource<ID, IDC, R, RRC> cached(
            Function<IDC, Publisher<R>> queryFunction,
            Map<ID, Collection<R>> delegateMap) {
        return cached(call(queryFunction), delegateMap);
    }

    static <ID, IDC extends Collection<ID>, R, RRC> CachedRuleMapperSource<ID, IDC, R, RRC> cached(
            RuleMapperSource<ID, IDC, R, RRC> ruleMapperSource,
            Map<ID, Collection<R>> delegateMap) {
        return ruleContext -> ids -> buildCache(ruleContext, ids, ruleMapperSource, delegateMap);
    }

    private static <ID, IDC extends Collection<ID>, R, RRC> Mono<Map<ID, Collection<R>>> buildCache(
            RuleContext<ID, IDC, R, RRC> ruleContext,
            Iterable<ID> ids,
            RuleMapperSource<ID, IDC, R, RRC> ruleMapperSource,
            Map<ID, Collection<R>> delegateMap) {

        return Mono.just(getAll(ids, delegateMap))
                .flatMap(cachedEntitiesMap -> then(intersect(ids, cachedEntitiesMap.keySet()), entityIds ->
                        entityIds.isEmpty() ? Mono.just(cachedEntitiesMap) :
                                Flux.from(ruleMapperSource.apply(ruleContext).apply(translate(entityIds, ruleContext.idCollectionFactory())))
                                        .collectMultimap(ruleContext.idExtractor())
                                        .doOnNext(map -> delegateMap.putAll(buildCacheFragment(entityIds, map)))
                                        .map(map -> mergeMaps(map, cachedEntitiesMap))));
    }

    private static <ID, R> Map<ID, Collection<R>> getAll(Iterable<ID> ids, Map<ID, Collection<R>> sourceMap) {
        return also(new HashMap<>(), copyMap -> ids.forEach(id -> ifNotNull(sourceMap.get(id), value -> copyMap.put(id, value))));
    }

    private static <ID, R> Map<ID, Collection<R>> buildCacheFragment(Set<ID> entityIds, Map<ID, Collection<R>> queryResultsMap) {
        return also(new HashMap<>(queryResultsMap), cacheFragment -> intersect(entityIds, cacheFragment.keySet())
                .forEach(id -> cacheFragment.put(id, List.of())));
    }
}
