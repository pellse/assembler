package io.github.pellse.assembler.caching;

import io.github.pellse.util.function.Function3;
import reactor.core.publisher.Mono;

import java.util.Map;

import static io.github.pellse.util.collection.CollectionUtils.isEmpty;
import static io.github.pellse.util.collection.CollectionUtils.nullToEmptyMap;
import static java.util.Map.of;
import static reactor.core.publisher.Mono.just;

public interface OptimizedCache {

    static <ID, RRC> Cache<ID, RRC> optimizedCache(Cache<ID, RRC> delegateCache) {

        return new Cache<>() {

            @Override
            public Mono<Map<ID, RRC>> getAll(Iterable<ID> ids) {
                return isEmpty(ids) ? just(of()) : delegateCache.getAll(ids);
            }

            @Override
            public Mono<Map<ID, RRC>> computeAll(Iterable<ID> ids, FetchFunction<ID, RRC> fetchFunction) {
                return isEmpty(ids) ? just(of()) : delegateCache.computeAll(ids, fetchFunction);
            }

            @Override
            public Mono<?> putAll(Map<ID, RRC> map) {
                return isEmpty(map) ? just(of()) : delegateCache.putAll(map);
            }

            @Override
            public <UUC> Mono<?> putAllWith(Map<ID, UUC> map, Function3<ID, RRC, UUC, RRC> mergeFunction) {
                return isEmpty(map) ? just(of()) : delegateCache.putAllWith(map, mergeFunction);
            }

            @Override
            public Mono<?> removeAll(Map<ID, RRC> map) {
                return isEmpty(map) ? just(of()) : delegateCache.removeAll(map);
            }

            @Override
            public Mono<?> updateAll(Map<ID, RRC> mapToAdd, Map<ID, RRC> mapToRemove) {
                return isEmpty(mapToAdd) && isEmpty(mapToRemove) ? just(of()) : delegateCache.updateAll(nullToEmptyMap(mapToAdd), nullToEmptyMap(mapToRemove));
            }
        };
    }
}
