package io.github.pellse.assembler.caching;

import io.github.pellse.assembler.caching.CacheFactory.CacheTransformer;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.github.pellse.assembler.caching.Cache.adapterCache;
import static io.github.pellse.util.lock.LockSupplier.executeWithLock;

public interface SerializeCacheFactory {

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheTransformer<ID, R, RRC, CTX> serialize() {
        return SerializeCacheFactory::serialize;
    }

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheFactory<ID, R, RRC, CTX> serialize(CacheFactory<ID, R, RRC, CTX> cacheFactory) {
        return context -> serialize(cacheFactory.create(context));
    }

    static <ID, RRC> Cache<ID, RRC> serialize(Cache<ID, RRC> delegateCache) {

        final var lock = new AtomicBoolean();

        return adapterCache(
                delegateCache::getAll,
                (ids, fetchFunction) -> executeWithLock(() -> delegateCache.computeAll(ids, fetchFunction), lock),
                delegateCache::putAll,
                delegateCache::removeAll,
                delegateCache::updateAll);
    }
}
