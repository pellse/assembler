package io.github.pellse.assembler.caching;

import io.github.pellse.assembler.caching.CacheFactory.CacheTransformer;
import reactor.core.publisher.Mono;

import static io.github.pellse.assembler.caching.Cache.adapterCache;

public interface DeferCacheFactory {

    static <ID, EID, R, RRC, CTX extends CacheContext<ID, EID, R, RRC>> CacheTransformer<ID, EID, R, RRC, CTX> defer() {
        return DeferCacheFactory::defer;
    }

    static <ID, EID, R, RRC, CTX extends CacheContext<ID, EID, R, RRC>> CacheFactory<ID, EID, R, RRC, CTX> defer(CacheFactory<ID, EID, R, RRC, CTX> cacheFactory) {
        return context -> deferCache(cacheFactory.create(context));
    }

    static <ID, RRC> Cache<ID, RRC> deferCache(Cache<ID, RRC> delegateCache) {
        return adapterCache(
                ids -> Mono.defer(() -> delegateCache.getAll(ids)),
                (ids, fetchFunction) -> Mono.defer(() -> delegateCache.computeAll(ids, fetchFunction)),
                map -> Mono.defer(() -> delegateCache.putAll(map)),
                map -> Mono.defer(() -> delegateCache.removeAll(map)),
                (mapToAdd, mapToRemove) -> Mono.defer(() -> delegateCache.updateAll(mapToAdd, mapToRemove)));
    }
}
