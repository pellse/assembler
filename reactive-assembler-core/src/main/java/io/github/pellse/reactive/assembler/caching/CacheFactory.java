package io.github.pellse.reactive.assembler.caching;

import io.github.pellse.reactive.assembler.RuleMapperSource;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static io.github.pellse.reactive.assembler.RuleMapperSource.call;
import static io.github.pellse.reactive.assembler.caching.Cache.cache;
import static io.github.pellse.reactive.assembler.caching.Cache.mergeStrategyAwareCache;
import static io.github.pellse.util.ObjectUtils.also;
import static io.github.pellse.util.ObjectUtils.then;
import static io.github.pellse.util.collection.CollectionUtil.*;
import static java.util.Arrays.stream;
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

    @SafeVarargs
    static <ID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, IDC, R, RRC> cached(
            Function<IDC, Publisher<R>> queryFunction,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {
        return cached(call(queryFunction), delegateCacheFactories);
    }

    @SafeVarargs
    static <ID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, IDC, R, RRC> cached(
            RuleMapperSource<ID, IDC, R, RRC> ruleMapperSource,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {
        return cached(ruleMapperSource, cache(), delegateCacheFactories);
    }

    @SafeVarargs
    static <ID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, IDC, R, RRC> cached(
            Function<IDC, Publisher<R>> queryFunction,
            Supplier<Map<ID, RRC>> mapSupplier,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {
        return cached(call(queryFunction), mapSupplier, delegateCacheFactories);
    }

    @SafeVarargs
    static <ID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, IDC, R, RRC> cached(
            RuleMapperSource<ID, IDC, R, RRC> ruleMapperSource,
            Supplier<Map<ID, RRC>> mapSupplier,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {
        return cached(ruleMapperSource, cache(mapSupplier), delegateCacheFactories);
    }

    @SafeVarargs
    static <ID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, IDC, R, RRC> cached(
            Function<IDC, Publisher<R>> queryFunction,
            Map<ID, RRC> delegateMap,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {
        return cached(call(queryFunction), delegateMap, delegateCacheFactories);
    }

    @SafeVarargs
    static <ID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, IDC, R, RRC> cached(
            RuleMapperSource<ID, IDC, R, RRC> ruleMapperSource,
            Map<ID, RRC> delegateMap,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {
        return cached(ruleMapperSource, cache(delegateMap), delegateCacheFactories);
    }

    @SafeVarargs
    static <ID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, IDC, R, RRC> cached(
            Function<IDC, Publisher<R>> queryFunction,
            CacheFactory<ID, R, RRC> cache,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {
        return cached(call(queryFunction), cache, delegateCacheFactories);
    }

    @SafeVarargs
    static <ID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, IDC, R, RRC> cached(
            RuleMapperSource<ID, IDC, R, RRC> ruleMapperSource,
            CacheFactory<ID, R, RRC> cacheFactory,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {

        return ruleContext -> {
            var queryFunction = ruleMapperSource.apply(ruleContext);
            Function<Iterable<? extends ID>, Mono<Map<ID, RRC>>> fetchFunction =
                    entityIds -> then(translate(entityIds, ruleContext.idCollectionFactory()), ids ->
                            Flux.from(queryFunction.apply(ids))
                                    .collect(ruleContext.mapCollector().apply(ids.size()))
                                    .map(queryResultsMap -> buildCacheFragment(ids, queryResultsMap, ruleContext.defaultResultProvider())));

            var cache = delegate(cacheFactory, delegateCacheFactories).create(
                    fetchFunction,
                    new Context<>(ruleContext.mapCollector(), ruleContext.mergeStrategy()));

            return ids -> cache.getAll(ids, true)
                    .flatMapMany(map -> fromStream(ruleContext.streamFlattener().apply(map.values().stream())));
        };
    }

    @SafeVarargs
    static <ID, R, RRC> CacheFactory<ID, R, RRC> delegate(
            CacheFactory<ID, R, RRC> cacheFactory,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {

        return also(stream(delegateCacheFactories).toList(), Collections::reverse).stream()
                .reduce((fetchFunction, context) -> mergeStrategyAwareCache(cacheFactory.create(fetchFunction, context), context.mergeStrategy()),
                        (mergeStrategyAwareCache, delegateWrapperFunction) -> delegateWrapperFunction.apply(mergeStrategyAwareCache),
                        (previousCacheFactory, decoratedCacheFactory) -> decoratedCacheFactory);
    }

    private static <ID, RRC> Map<ID, RRC> buildCacheFragment(
            Iterable<? extends ID> entityIds,
            Map<ID, RRC> queryResultsMap,
            Function<ID, RRC> defaultResultProvider) {

        return newMap(queryResultsMap, map ->
                intersect(entityIds, map.keySet()).forEach(id -> map.put(id, defaultResultProvider.apply(id))));
    }
}
