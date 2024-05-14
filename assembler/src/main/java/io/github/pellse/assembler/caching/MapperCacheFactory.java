package io.github.pellse.assembler.caching;

import io.github.pellse.assembler.caching.CacheFactory.CacheTransformer;

import java.util.function.BiFunction;
import java.util.function.Function;

import static io.github.pellse.assembler.caching.Cache.adapterCache;
import static io.github.pellse.util.ObjectUtils.then;
import static io.github.pellse.util.collection.CollectionUtils.transformMap;

public interface MapperCacheFactory {

    static <ID, EID, R, RRC, CTX extends CacheContext<ID, EID, R, RRC>> CacheTransformer<ID, EID, R, RRC, CTX> mapper(Function<CTX, BiFunction<ID, RRC, RRC>> mappingFunction) {
        return cacheFactory -> mapper(cacheFactory, mappingFunction);
    }

    static <ID, EID, R, RRC, CTX extends CacheContext<ID, EID, R, RRC>> CacheFactory<ID, EID, R, RRC, CTX> mapper(CacheFactory<ID, EID, R, RRC, CTX> cacheFactory, Function<CTX, BiFunction<ID, RRC, RRC>> mappingFunction) {
        return context -> then(cacheFactory.create(context), delegateCache -> adapterCache(
                delegateCache::getAll,
                (ids, fetchFunction) -> delegateCache.computeAll(ids, idList -> fetchFunction.apply(idList).map(m -> transformMap(m, mappingFunction.apply(context)))),
                map -> delegateCache.putAll(transformMap(map, mappingFunction.apply(context))),
                delegateCache::removeAll,
                (mapToAdd, mapToRemove) -> delegateCache.updateAll(transformMap(mapToAdd, mappingFunction.apply(context)), mapToRemove)
        ));
    }
}