package io.github.pellse.assembler.caching;

import io.github.pellse.assembler.caching.factory.CacheContext.OneToManyCacheContext;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.github.pellse.assembler.caching.OptimizedCache.optimizedCache;
import static io.github.pellse.util.ObjectUtils.then;
import static io.github.pellse.util.collection.CollectionUtils.*;
import static java.util.Map.of;
import static reactor.core.publisher.Mono.just;

public interface OneToManyCache {

    @FunctionalInterface
    interface MergeStrategy<ID, RRC> {
        Map<ID, RRC> merge(Map<ID, RRC> cache, Map<ID, RRC> itemsToUpdateMap);
    }

    @FunctionalInterface
    interface CacheUpdater<ID, RRC> {
        Mono<?> updateCache(Cache<ID, RRC> cache, Map<ID, RRC> existingCacheItems, Map<ID, RRC> incomingChanges);
    }

    static <ID, EID, R> Cache<ID, List<R>> oneToManyCache(
            OneToManyCacheContext<ID, EID, R> ctx,
            Cache<ID, List<R>> delegateCache) {

        final var optimizedCache = optimizedCache(delegateCache);

        return new Cache<>() {

            @Override
            public Mono<Map<ID, List<R>>> getAll(Iterable<ID> ids) {
                return optimizedCache.getAll(ids);
            }

            @Override
            public Mono<Map<ID, List<R>>> computeAll(Iterable<ID> ids, FetchFunction<ID, List<R>> fetchFunction) {
                return optimizedCache.computeAll(ids, fetchFunction);
            }

            @Override
            public Mono<?> putAll(Map<ID, List<R>> map) {
                return applyMergeStrategy(
                        optimizedCache,
                        (existingCacheItems, incomingChanges) -> ctx.mapMerger().apply(existingCacheItems, incomingChanges),
                        Cache::putAll)
                        .apply(map);
            }

            @Override
            public Mono<?> removeAll(Map<ID, List<R>> map) {
                return applyMergeStrategy(
                        optimizedCache,
                        (cache, existingCacheItems, incomingChanges) ->
                                then(subtractFromMap(incomingChanges, existingCacheItems, ctx.idResolver()),
                                        updatedMap -> cache.updateAll(updatedMap, diff(existingCacheItems, updatedMap))))
                        .apply(map);
            }
        };
    }

    private static <ID, R> Function<Map<ID, List<R>>, Mono<?>> applyMergeStrategy(
            Cache<ID, List<R>> delegateCache,
            MergeStrategy<ID, List<R>> mergeStrategy,
            BiFunction<Cache<ID, List<R>>, Map<ID, List<R>>, Mono<?>> cacheUpdater) {

        return applyMergeStrategy(
                delegateCache,
                (cache, existingCacheItems, incomingChanges) ->
                        cacheUpdater.apply(cache, mergeStrategy.merge(existingCacheItems, incomingChanges)));
    }

    private static <ID, R> Function<Map<ID, List<R>>, Mono<?>> applyMergeStrategy(
            Cache<ID, List<R>> delegateCache,
            CacheUpdater<ID, List<R>> cacheUpdater) {

        return incomingChanges -> isEmpty(incomingChanges) ? just(of()) : delegateCache.getAll(incomingChanges.keySet())
                .flatMap(existingCacheItems -> cacheUpdater.updateCache(delegateCache, existingCacheItems, incomingChanges));
    }
}
