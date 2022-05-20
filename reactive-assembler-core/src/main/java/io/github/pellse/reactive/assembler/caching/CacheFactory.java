package io.github.pellse.reactive.assembler.caching;

import io.github.pellse.reactive.assembler.RuleMapperSource;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static io.github.pellse.reactive.assembler.caching.Cache.cache;
import static io.github.pellse.reactive.assembler.RuleMapperSource.call;
import static io.github.pellse.util.ObjectUtils.then;
import static io.github.pellse.util.collection.CollectionUtil.*;
import static reactor.core.publisher.Flux.fromStream;

@FunctionalInterface
public interface CacheFactory<ID, R, RRC> {

    record Context<ID, R, RRC>(
            Function<Integer, Collector<R, ?, Map<ID, RRC>>> mapCollector,
            MergeStrategy<ID, RRC> mergeStrategy) {
    }

    Cache<ID, RRC> create(
            Function<Iterable<? extends ID>, Mono<Map<ID, RRC>>> fetchFunction,
            Context<ID, R, RRC> context);

    static <ID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, IDC, R, RRC> cached(Function<IDC, Publisher<R>> queryFunction) {
        return cached(call(queryFunction));
    }

    static <ID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, IDC, R, RRC> cached(RuleMapperSource<ID, IDC, R, RRC> ruleMapperSource) {
        return cached(ruleMapperSource, cache());
    }

    static <ID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, IDC, R, RRC> cached(
            Function<IDC, Publisher<R>> queryFunction,
            Supplier<Map<ID, RRC>> mapSupplier) {
        return cached(call(queryFunction), mapSupplier);
    }

    static <ID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, IDC, R, RRC> cached(
            RuleMapperSource<ID, IDC, R, RRC> ruleMapperSource,
            Supplier<Map<ID, RRC>> mapSupplier) {
        return cached(ruleMapperSource, cache(mapSupplier));
    }

    static <ID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, IDC, R, RRC> cached(
            Function<IDC, Publisher<R>> queryFunction,
            Map<ID, RRC> delegateMap) {
        return cached(call(queryFunction), delegateMap);
    }

    static <ID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, IDC, R, RRC> cached(
            RuleMapperSource<ID, IDC, R, RRC> ruleMapperSource,
            Map<ID, RRC> delegateMap) {
        return cached(ruleMapperSource, cache(delegateMap));
    }

    static <ID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, IDC, R, RRC> cached(
            Function<IDC, Publisher<R>> queryFunction,
            CacheFactory<ID, R, RRC> cache) {
        return cached(call(queryFunction), cache);
    }

    static <ID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, IDC, R, RRC> cached(
            RuleMapperSource<ID, IDC, R, RRC> ruleMapperSource,
            CacheFactory<ID, R, RRC> cacheFactory) {

        return ruleContext -> {
            var queryFunction = ruleMapperSource.apply(ruleContext);
            Function<Iterable<? extends ID>, Mono<Map<ID, RRC>>> fetchFunction =
                    entityIds -> then(translate(entityIds, ruleContext.idCollectionFactory()), ids ->
                            Flux.from(queryFunction.apply(ids))
                                    .collect(ruleContext.mapCollector().apply(ids.size()))
                                    .map(queryResultsMap -> buildCacheFragment(ids, queryResultsMap, ruleContext.defaultResultProvider())));

            final var cache = cacheFactory.create(
                    fetchFunction,
                    new Context<>(ruleContext.mapCollector(), ruleContext.mergeStrategy()));

            return ids -> cache.getAll(ids, true)
                    .flatMapMany(map -> fromStream(ruleContext.streamFlattener().apply(map.values().stream())));
        };
    }

    private static <ID, RRC> Map<ID, RRC> buildCacheFragment(
            Iterable<? extends ID> entityIds,
            Map<ID, RRC> queryResultsMap,
            Function<ID, RRC> defaultResultProvider) {

        return newMap(queryResultsMap, map ->
                intersect(entityIds, map.keySet()).forEach(id -> map.put(id, defaultResultProvider.apply(id))));
    }
}
