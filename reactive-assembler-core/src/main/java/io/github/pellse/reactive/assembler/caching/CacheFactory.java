package io.github.pellse.reactive.assembler.caching;

import io.github.pellse.reactive.assembler.RuleMapperContext;
import io.github.pellse.reactive.assembler.RuleMapperSource;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.github.pellse.reactive.assembler.RuleMapperSource.call;
import static io.github.pellse.reactive.assembler.RuleMapperSource.emptyQuery;
import static io.github.pellse.reactive.assembler.caching.Cache.cache;
import static io.github.pellse.reactive.assembler.caching.Cache.mergeStrategyAwareCache;
import static io.github.pellse.util.ObjectUtils.*;
import static io.github.pellse.util.collection.CollectionUtil.*;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.groupingBy;
import static reactor.core.publisher.Flux.fromStream;
import static reactor.core.publisher.Mono.just;

@FunctionalInterface
public interface CacheFactory<ID, R, RRC> {

    Cache<ID, R> create(
            Function<Iterable<? extends ID>, Mono<Map<ID, List<R>>>> fetchFunction,
            CacheContext<ID, R, RRC> context);

    record CacheContext<ID, R, RRC>(
            Function<R, ID> correlationIdExtractor,
            Function<List<R>, RRC> fromListConverter,
            Function<RRC, List<R>> toListConverter) {

        public CacheContext(RuleMapperContext<ID, ?, ?, R, RRC> ctx) {
            this(ctx.correlationIdExtractor(), ctx.fromListConverter(), ctx.toListConverter());
        }
    }

    interface CacheTransformer<ID, R, RRC> extends Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> {
    }

    @SafeVarargs
    static <ID, EID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, EID, IDC, R, RRC> cached(
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {
        return cached(cache(), delegateCacheFactories);
    }

    @SafeVarargs
    static <ID, EID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, EID, IDC, R, RRC> cached(
            Supplier<Map<ID, List<R>>> mapSupplier,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {
        return cached(emptyQuery(), mapSupplier, delegateCacheFactories);
    }

    @SafeVarargs
    static <ID, EID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, EID, IDC, R, RRC> cached(
            Map<ID, List<R>> delegateMap,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {
        return cached(emptyQuery(), delegateMap, delegateCacheFactories);
    }

    @SafeVarargs
    static <ID, EID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, EID, IDC, R, RRC> cached(
            CacheFactory<ID, R, RRC> cache,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {
        return cached(emptyQuery(), cache, delegateCacheFactories);
    }

    @SafeVarargs
    static <ID, EID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, EID, IDC, R, RRC> cached(
            Function<IDC, Publisher<R>> queryFunction,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {
        return cached(call(queryFunction), delegateCacheFactories);
    }

    @SafeVarargs
    static <ID, EID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, EID, IDC, R, RRC> cached(
            RuleMapperSource<ID, EID, IDC, R, RRC> ruleMapperSource,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {
        return cached(ruleMapperSource, cache(), delegateCacheFactories);
    }

    @SafeVarargs
    static <ID, EID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, EID, IDC, R, RRC> cached(
            Function<IDC, Publisher<R>> queryFunction,
            Supplier<Map<ID, List<R>>> mapSupplier,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {
        return cached(call(queryFunction), mapSupplier, delegateCacheFactories);
    }

    @SafeVarargs
    static <ID, EID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, EID, IDC, R, RRC> cached(
            RuleMapperSource<ID, EID, IDC, R, RRC> ruleMapperSource,
            Supplier<Map<ID, List<R>>> mapSupplier,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {
        return cached(ruleMapperSource, cache(mapSupplier), delegateCacheFactories);
    }

    @SafeVarargs
    static <ID, EID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, EID, IDC, R, RRC> cached(
            Function<IDC, Publisher<R>> queryFunction,
            Map<ID, List<R>> delegateMap,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {
        return cached(call(queryFunction), delegateMap, delegateCacheFactories);
    }

    @SafeVarargs
    static <ID, EID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, EID, IDC, R, RRC> cached(
            RuleMapperSource<ID, EID, IDC, R, RRC> ruleMapperSource,
            Map<ID, List<R>> delegateMap,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {
        return cached(ruleMapperSource, cache(delegateMap), delegateCacheFactories);
    }

    @SafeVarargs
    static <ID, EID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, EID, IDC, R, RRC> cached(
            Function<IDC, Publisher<R>> queryFunction,
            CacheFactory<ID, R, RRC> cacheFactory,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {
        return cached(call(queryFunction), cacheFactory, delegateCacheFactories);
    }

    @SafeVarargs
    static <ID, EID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, EID, IDC, R, RRC> cached(
            RuleMapperSource<ID, EID, IDC, R, RRC> ruleMapperSource,
            CacheFactory<ID, R, RRC> cacheFactory,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {

        return ruleContext -> {
            var queryFunction = ruleMapperSource.apply(ruleContext);
            Function<Iterable<? extends ID>, Mono<Map<ID, List<R>>>> fetchFunction =
                    entityIds -> then(translate(entityIds, ruleContext.idCollectionFactory()), ids ->
                            Flux.from(queryFunction.apply(ids))
                                    .collect(groupingBy(ruleContext.correlationIdExtractor()))
                                    .map(queryResultsMap -> buildCacheFragment(entityIds, queryResultsMap, ruleContext)));

            var cache = delegate(ruleContext, cacheFactory, delegateCacheFactories)
                    .create(fetchFunction, new CacheContext<>(ruleContext));

            return ids -> cache.getAll(ids, true)
                    .flatMapMany(map -> fromStream(map.values().stream().flatMap(Collection::stream)));
        };
    }

    static <ID, RRC> Function<Map<ID, RRC>, Mono<?>> toMono(Consumer<Map<ID, RRC>> consumer) {
        return map -> just(also(map, consumer));
    }

    @SafeVarargs
    private static <ID, EID, IDC extends Collection<ID>, R, RRC> CacheFactory<ID, R, RRC> delegate(
            RuleMapperContext<ID, EID, IDC, R, RRC> ruleContext,
            CacheFactory<ID, R, RRC> cacheFactory,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {

        return stream(delegateCacheFactories)
                .reduce((fetchFunction, context) -> mergeStrategyAwareCache(ruleContext.idExtractor(), cacheFactory.create(fetchFunction, context)),
                        (previousCacheFactory, delegateWrapperFunction) -> delegateWrapperFunction.apply(previousCacheFactory),
                        (previousCacheFactory, decoratedCacheFactory) -> decoratedCacheFactory);
    }

    private static <ID, EID, IDC extends Collection<ID>, R, RRC> Map<ID, List<R>> buildCacheFragment(
            Iterable<? extends ID> entityIds,
            Map<ID, List<R>> queryResultsMap,
            RuleMapperContext<ID, EID, IDC, R, RRC> ctx) {

        return newMap(queryResultsMap, map ->
                intersect(entityIds, map.keySet()).forEach(id ->
                        ifNotNull(ctx.defaultResultProvider().apply(id), value -> map.put(id, ctx.toListConverter().apply(value)))));
    }
}
