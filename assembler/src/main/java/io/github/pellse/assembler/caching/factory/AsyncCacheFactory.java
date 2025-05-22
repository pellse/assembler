package io.github.pellse.assembler.caching.factory;

import io.github.pellse.assembler.caching.Cache;
import io.github.pellse.util.function.Function3;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Set;

import static io.github.pellse.util.collection.CollectionUtils.diff;
import static io.github.pellse.util.collection.CollectionUtils.isEmpty;
import static java.util.concurrent.ConcurrentHashMap.newKeySet;

public interface AsyncCacheFactory {

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheTransformer<ID, R, RRC, CTX> async() {
        return AsyncCacheFactory::async;
    }

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheFactory<ID, R, RRC, CTX> async(CacheFactory<ID, R, RRC, CTX> cacheFactory) {
        return context -> async(cacheFactory.create(context));
    }

    static <ID, RRC> Cache<ID, RRC> async(Cache<ID, RRC> delegateCache) {

        final Set<ID> idSet = newKeySet();

        return new Cache<>() {

            @Override
            public Mono<Map<ID, RRC>> getAll(Iterable<ID> ids) {
                return delegateCache.getAll(ids);
            }

            @Override
            public Mono<Map<ID, RRC>> computeAll(Iterable<ID> ids, FetchFunction<ID, RRC> fetchFunction) {

                final var missingIds = diff(ids, idSet);
                if (isEmpty(missingIds)) {
                    return delegateCache.getAll(ids);
                }

                idSet.addAll(missingIds);

                return delegateCache.computeAll(ids, fetchFunction)
                        .doOnNext(map -> idSet.removeAll(diff(idSet, map.keySet())))
                        .doOnError(error -> idSet.removeAll(missingIds))
                        .doOnCancel(() -> idSet.removeAll(missingIds));
            }

            @Override
            public Mono<?> putAll(Map<ID, RRC> map) {
                return delegateCache.putAll(map);
            }

            @Override
            public <UUC> Mono<?> putAllWith(Map<ID, UUC> map, Function3<ID, RRC, UUC, RRC> mergeFunction) {
                return delegateCache.putAllWith(map, mergeFunction);
            }

            @Override
            public Mono<?> removeAll(Map<ID, RRC> map) {
                return delegateCache.removeAll(map);
            }

            @Override
            public Mono<?> updateAll(Map<ID, RRC> mapToAdd, Map<ID, RRC> mapToRemove) {
                return delegateCache.updateAll(mapToAdd, mapToRemove);
            }
        };
    }
}
