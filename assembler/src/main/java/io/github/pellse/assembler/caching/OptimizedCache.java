package io.github.pellse.assembler.caching;

import io.github.pellse.assembler.caching.Cache.FetchFunction;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.github.pellse.util.collection.CollectionUtils.isEmpty;
import static io.github.pellse.util.collection.CollectionUtils.nullToEmptyMap;
import static java.util.Map.of;
import static reactor.core.publisher.Mono.just;

public interface OptimizedCache {

    static <ID, RRC> Cache<ID, RRC> optimizedCache(Cache<ID, RRC> delegateCache) {

        return new Cache<>() {
            @Override
            public Mono<Map<ID, RRC>> getAll(Iterable<ID> ids) {
                return emptyOr(delegateCache::getAll).apply(ids);
            }

            @Override
            public Mono<Map<ID, RRC>> computeAll(Iterable<ID> ids, FetchFunction<ID, RRC> fetchFunction) {
                return emptyOr(delegateCache::computeAll).apply(ids, fetchFunction);
            }

            @Override
            public Mono<?> putAll(Map<ID, RRC> map) {
                return emptyMapOr(delegateCache::putAll).apply(map);
            }

            @Override
            public Mono<?> removeAll(Map<ID, RRC> map) {
                return emptyMapOr(delegateCache::removeAll).apply(map);
            }

            @Override
            public Mono<?> updateAll(Map<ID, RRC> mapToAdd, Map<ID, RRC> mapToRemove) {
                return emptyMapOr(delegateCache::updateAll).apply(mapToAdd, mapToRemove);
            }
        };
    }

    private static <ID, RRC> Function<Iterable<ID>, Mono<Map<ID, RRC>>> emptyOr(Function<Iterable<ID>, Mono<Map<ID, RRC>>> mappingFunction) {
        return ids -> isEmpty(ids) ? just(of()) : mappingFunction.apply(ids);
    }

    private static <ID, RRC> BiFunction<Iterable<ID>, FetchFunction<ID, RRC>, Mono<Map<ID, RRC>>> emptyOr(
            BiFunction<Iterable<ID>, FetchFunction<ID, RRC>, Mono<Map<ID, RRC>>> mappingFunction) {

        return (ids, fetchFunction) -> isEmpty(ids) ? just(of()) : mappingFunction.apply(ids, fetchFunction);
    }

    private static <ID, RRC> Function<Map<ID, RRC>, Mono<?>> emptyMapOr(Function<Map<ID, RRC>, Mono<?>> mappingFunction) {
        return map -> isEmpty(map) ? just(of()) : mappingFunction.apply(map);
    }

    private static <ID, RRC> BiFunction<Map<ID, RRC>, Map<ID, RRC>, Mono<?>> emptyMapOr(BiFunction<Map<ID, RRC>, Map<ID, RRC>, Mono<?>> mappingFunction) {
        return (map1, map2) -> isEmpty(map1) && isEmpty(map2) ? just(of()) : mappingFunction.apply(nullToEmptyMap(map1), nullToEmptyMap(map2));
    }
}
