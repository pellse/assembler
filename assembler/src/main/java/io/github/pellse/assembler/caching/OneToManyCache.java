package io.github.pellse.assembler.caching;

import io.github.pellse.assembler.caching.factory.CacheContext.OneToManyCacheContext;
import io.github.pellse.util.function.Function3;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

import static io.github.pellse.assembler.caching.OptimizedCache.optimizedCache;
import static io.github.pellse.util.collection.CollectionUtils.*;

public interface OneToManyCache {

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
                return putAllWith(map, (id, coll1, coll2) -> removeDuplicates(concat(coll1, coll2), ctx.idResolver()));
            }

            @Override
            public <UC> Mono<?> putAllWith(Map<ID, UC> map, Function3<ID, List<R>, UC, List<R>> mergeFunction) {
                return optimizedCache.putAllWith(map, mergeFunction);
            }

            @Override
            public Mono<?> removeAll(Map<ID, List<R>> map) {
                return optimizedCache.getAll(map.keySet())
                        .flatMap(existingCacheItems -> {
                            final var updatedMap = subtractFromMap(map, existingCacheItems, ctx.idResolver());
                            return optimizedCache.updateAll(updatedMap, diff(existingCacheItems, updatedMap));
                        });
            }
        };
    }
}
