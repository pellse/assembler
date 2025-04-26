package io.github.pellse.assembler.caching;

import io.github.pellse.assembler.caching.Cache.MergeFunction;
import reactor.core.publisher.Mono;

import java.util.Map;

import static io.github.pellse.assembler.caching.OptimizedCache.optimizedCache;
import static io.github.pellse.util.collection.CollectionUtils.mergeMaps;

public interface OneToOneCache {

    static <ID, R> Cache<ID, R> oneToOneCache(Cache<ID, R> delegateCache) {
        return oneToOneCache((k, r1, r2) -> r2 != null ? r2 : r1, delegateCache);
    }

    static <ID, R> Cache<ID, R> oneToOneCache(
            MergeFunction<ID, R> mergeFunction,
            Cache<ID, R> delegateCache) {

        final var optimizedCache = optimizedCache(delegateCache);

        return new Cache<>() {
            @Override
            public Mono<Map<ID, R>> getAll(Iterable<ID> ids) {
                return optimizedCache.getAll(ids);
            }

            @Override
            public Mono<Map<ID, R>> computeAll(Iterable<ID> ids, FetchFunction<ID, R> fetchFunction) {
                return optimizedCache.computeAll(ids, fetchFunction);
            }

            @Override
            public Mono<?> putAll(Map<ID, R> map) {
                return optimizedCache.getAll(map.keySet())
                        .map(existingMap -> mergeMaps(existingMap, map, mergeFunction))
                        .flatMap(optimizedCache::putAll);
            }

            @Override
            public Mono<?> removeAll(Map<ID, R> map) {
                return optimizedCache.removeAll(map);
            }
        };
    }
}
