package io.github.pellse.assembler.caching;

import io.github.pellse.assembler.caching.factory.CacheContext;

import static io.github.pellse.assembler.caching.AdapterCache.adapterCache;
import static io.github.pellse.assembler.caching.OptimizedCache.optimizedCache;

public interface OneToOneCache {

    static <ID, R> Cache<ID, R> oneToOneCache(
            CacheContext.OneToOneCacheContext<ID, R, R> ctx,
            Cache<ID, R> delegateCache) {

        final var optimizedCache = optimizedCache(delegateCache);

        return adapterCache(
                optimizedCache::getAll,
                optimizedCache::computeAll,
                newMap -> optimizedCache.getAll(newMap.keySet())
                        .map(existingMap -> ctx.mapMerger().apply(existingMap, newMap))
                        .flatMap(optimizedCache::putAll),
                optimizedCache::removeAll);
    }
}
