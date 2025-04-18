package io.github.pellse.assembler.caching;

import io.github.pellse.assembler.caching.factory.CacheContext.OneToManyCacheContext;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.github.pellse.assembler.caching.AdapterCache.adapterCache;
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

    static <ID,  EID, R, RC extends Collection<R>> Cache<ID, RC> oneToManyCache(
            OneToManyCacheContext<ID, EID, R, RC, R, RC> ctx,
            Cache<ID, RC> delegateCache) {

        final var optimizedCache = optimizedCache(delegateCache);

        return adapterCache(
                optimizedCache::getAll,
                optimizedCache::computeAll,
                applyMergeStrategy(
                        optimizedCache,
                        (existingCacheItems, incomingChanges) -> ctx.mapMerger().apply(existingCacheItems, incomingChanges),
                        Cache::putAll),
                applyMergeStrategy(
                        optimizedCache,
                        (cache, existingCacheItems, incomingChanges) ->
                                then(subtractFromMap(incomingChanges, existingCacheItems, ctx.idResolver(), ctx.collectionFactory()),
                                        updatedMap -> cache.updateAll(updatedMap, diff(existingCacheItems, updatedMap))))
//                (incomingChangesToAdd, incomingChangesToRemove) -> {
//                    delegateCache.getAll(Stream.concat(incomingChangesToAdd.keySet().stream(), incomingChangesToRemove.keySet().stream()).distinct().toList())
//                            .flatMap(existingCacheItems -> )
//
//
//                    return just(of());
//                }
        );
    }

    private static <ID, R, RC extends Collection<R>> Function<Map<ID, RC>, Mono<?>> applyMergeStrategy(
            Cache<ID, RC> delegateCache,
            MergeStrategy<ID, RC> mergeStrategy,
            BiFunction<Cache<ID, RC>, Map<ID, RC>, Mono<?>> cacheUpdater) {

        return applyMergeStrategy(
                delegateCache,
                (cache, existingCacheItems, incomingChanges) ->
                        cacheUpdater.apply(cache, mergeStrategy.merge(existingCacheItems, incomingChanges)));
    }

    private static <ID, R, RC extends Collection<R>> Function<Map<ID, RC>, Mono<?>> applyMergeStrategy(
            Cache<ID, RC> delegateCache,
            CacheUpdater<ID, RC> cacheUpdater) {

        return incomingChanges -> isEmpty(incomingChanges) ? just(of()) : delegateCache.getAll(incomingChanges.keySet())
                .flatMap(existingCacheItems -> cacheUpdater.updateCache(delegateCache, existingCacheItems, incomingChanges));
    }
}
