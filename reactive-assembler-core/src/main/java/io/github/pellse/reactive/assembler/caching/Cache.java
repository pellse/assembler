package io.github.pellse.reactive.assembler.caching;

import io.github.pellse.reactive.assembler.RuleMapperContext;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.github.pellse.reactive.assembler.caching.AdapterCache.adapterCache;
import static io.github.pellse.reactive.assembler.caching.CacheFactory.toMono;
import static io.github.pellse.util.ObjectUtils.then;
import static io.github.pellse.util.collection.CollectionUtil.*;
import static java.util.Map.of;
import static reactor.core.publisher.Mono.just;

@FunctionalInterface
interface CacheUpdater<ID, R> {
    Mono<?> updateCache(Cache<ID, R> cache, Map<ID, List<R>> cacheQueryResults, Map<ID, List<R>> incomingChanges);
}

public interface Cache<ID, R> {

    Mono<Map<ID, List<R>>> getAll(Iterable<ID> ids, boolean computeIfAbsent);

    Mono<?> putAll(Map<ID, List<R>> map);

    Mono<?> removeAll(Map<ID, List<R>> map);

    default Mono<?> updateAll(Map<ID, List<R>> mapToAdd, Map<ID, List<R>> mapToRemove) {
        return putAll(mapToAdd).then(removeAll(mapToRemove));
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> cache() {
        return cache(ConcurrentHashMap::new);
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> cache(Supplier<Map<ID, List<R>>> mapSupplier) {
        return cache(mapSupplier.get());
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> cache(Map<ID, List<R>> delegateMap) {

        return (fetchFunction, __) -> adapterCache(
                (ids, computeIfAbsent) -> just(readAll(ids, delegateMap))
                        .flatMap(cachedEntitiesMap -> then(intersect(ids, cachedEntitiesMap.keySet()), entityIds ->
                                !computeIfAbsent || entityIds.isEmpty() ? just(cachedEntitiesMap) :
                                        fetchFunction.apply(entityIds)
                                                .doOnNext(delegateMap::putAll)
                                                .map(map -> mergeMaps(map, cachedEntitiesMap)))),
                toMono(delegateMap::putAll),
                toMono(map -> delegateMap.keySet().removeAll(map.keySet()))
        );
    }

    static <ID, EID, IDC extends Collection<ID>, R, RRC> Cache<ID, R> mergeStrategyAwareCache(
            RuleMapperContext<ID, EID, IDC, R, RRC> ruleContext,
            Cache<ID, R> delegateCache) {

        Cache<ID, R> optimizedCache = adapterCache(
                emptyOr(delegateCache::getAll),
                emptyOr(delegateCache::putAll),
                emptyOr(delegateCache::removeAll)
        );

        var idExtractor = ruleContext.idExtractor();

        return adapterCache(
                optimizedCache::getAll,
                applyMergeStrategy(
                        optimizedCache,
                        (cacheQueryResults, incomingChanges) -> mergeMaps(incomingChanges, cacheQueryResults, idExtractor),
                        Cache::putAll),
                applyMergeStrategy(
                        optimizedCache,
                        (cache, cacheQueryResults, incomingChanges) ->
                                then(removeFromMap(incomingChanges, cacheQueryResults, idExtractor),
                                        updatedMap -> cache.putAll(updatedMap)
                                                .then(cache.removeAll(diff(cacheQueryResults, updatedMap)))))
        );
    }

    private static <ID, R> Function<Map<ID, List<R>>, Mono<?>> applyMergeStrategy(
            Cache<ID, R> delegateCache,
            MergeStrategy<ID, R> mergeStrategy,
            BiFunction<Cache<ID, R>, Map<ID, List<R>>, Mono<?>> cacheUpdater) {

        return applyMergeStrategy(
                delegateCache,
                (cache, cacheQueryResults, incomingChanges) ->
                        cacheUpdater.apply(cache, mergeStrategy.merge(cacheQueryResults, incomingChanges)));
    }

    private static <ID, R> Function<Map<ID, List<R>>, Mono<?>> applyMergeStrategy(
            Cache<ID, R> delegateCache,
            CacheUpdater<ID, R> cacheUpdater) {

        return incomingChanges -> isEmpty(incomingChanges) ? just(of()) :
                delegateCache.getAll(incomingChanges.keySet(), false)
                        .flatMap(cacheQueryResults -> cacheUpdater.updateCache(delegateCache, cacheQueryResults, incomingChanges));
    }

    private static <ID, R> Function<Map<ID, List<R>>, Mono<?>> emptyOr(
            Function<Map<ID, List<R>>, Mono<?>> mappingFunction) {
        return map -> isEmpty(map) ? just(of()) : mappingFunction.apply(map);
    }

    private static <ID, R> BiFunction<Iterable<ID>, Boolean, Mono<Map<ID, List<R>>>> emptyOr(
            BiFunction<Iterable<ID>, Boolean, Mono<Map<ID, List<R>>>> mappingFunction) {
        return (ids, computeIfAbsent) -> isEmpty(ids) ? just(of()) : mappingFunction.apply(ids, computeIfAbsent);
    }
}
