package io.github.pellse.assembler.caching.factory;

import io.github.pellse.assembler.caching.Cache;
import io.github.pellse.util.function.Function3;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

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

        return new Cache<>() {

            @Override
            public Mono<Map<ID, RRC>> getAll(Iterable<ID> ids) {
                return delegateCache.getAll(ids);
            }

            @Override
            public Mono<Map<ID, RRC>> computeAll(Iterable<ID> ids, FetchFunction<ID, RRC> fetchFunction) {
                return executeWithLock(() -> delegateCache.computeAll(ids, fetchFunction), lock);
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
