package io.github.pellse.assembler.caching;

import io.github.pellse.util.function.Function3;

import static io.github.pellse.assembler.caching.AdapterCache.adapterCache;
import static io.github.pellse.assembler.caching.OptimizedCache.optimizedCache;
import static io.github.pellse.util.collection.CollectionUtils.mergeMaps;

public interface OneToOneCache {

    static <ID, R> Cache<ID, R> oneToOneCache(Cache<ID, R> delegateCache) {
        return oneToOneCache((k, r1, r2) -> r2 != null ? r2 : r1, delegateCache);
    }

    static <ID, R> Cache<ID, R> oneToOneCache(
            Function3<ID, R, R, R> mergeFunction,
            Cache<ID, R> delegateCache) {

        final var optimizedCache = optimizedCache(delegateCache);

        return adapterCache(
                optimizedCache::getAll,
                optimizedCache::computeAll,
                newMap -> optimizedCache.getAll(newMap.keySet())
                        .map(existingMap -> mergeMaps(existingMap, newMap, mergeFunction))
                        .flatMap(optimizedCache::putAll),
                optimizedCache::removeAll);
    }
}
