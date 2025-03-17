package io.github.pellse.assembler.caching;

import io.github.pellse.assembler.caching.Cache.FetchFunction;
import io.github.pellse.assembler.caching.CacheFactory.CacheTransformer;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static io.github.pellse.assembler.caching.Cache.adapterCache;
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

        BiFunction<Iterable<ID>, FetchFunction<ID, RRC>, Mono<Map<ID, RRC>>> computeAll = (ids, fetchFunction) -> {
            final var missingIds = diff(ids, idSet);
            if (isEmpty(missingIds)) {
                return delegateCache.getAll(ids);
            }

            idSet.addAll(missingIds);

            return delegateCache.computeAll(ids, fetchFunction)
                    .doOnNext(map -> idSet.removeAll(diff(idSet, map.keySet())))
                    .doOnError(error -> idSet.removeAll(missingIds))
                    .doOnCancel(() -> idSet.removeAll(missingIds));
        };

        return adapterCache(
                delegateCache::getAll,
                computeAll,
                delegateCache::putAll,
                delegateCache::removeAll,
                delegateCache::updateAll);
    }
}
