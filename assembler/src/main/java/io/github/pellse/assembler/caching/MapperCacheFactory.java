package io.github.pellse.assembler.caching;

import io.github.pellse.assembler.caching.CacheFactory.CacheTransformer;

import java.util.Collection;
import java.util.function.BiFunction;

import static io.github.pellse.assembler.caching.Cache.adapterCache;
import static io.github.pellse.util.ObjectUtils.then;
import static io.github.pellse.util.collection.CollectionUtils.transformMap;

public interface MapperCacheFactory {

    static <ID, EID, R, RRC> CacheTransformer<ID, EID, R, RRC> mapper(BiFunction<ID, RRC, RRC> mappingFunction) {
        return cacheFactory -> context -> then(cacheFactory.create(context), delegateCache -> adapterCache(
                delegateCache::getAll,
                delegateCache::computeAll,
                map -> delegateCache.putAll(transformMap(map, mappingFunction)),
                delegateCache::removeAll,
                (mapToAdd, mapToRemove ) -> delegateCache.updateAll(transformMap(mapToAdd, mappingFunction), mapToRemove)
        ));
    }

    static <ID, EID, R, RC extends Collection<R>> CacheTransformer<ID, EID, R, RC> oneToManyMapper(BiFunction<ID, RC, ? extends Collection<R>> mappingFunction) {
        return cacheFactory -> context -> then(cacheFactory.create(context), delegateCache -> adapterCache(
                delegateCache::getAll,
                delegateCache::computeAll,
                map -> delegateCache.putAll(transformMap(map, (id, coll) -> context.mappingFunction.apply(id, coll))),
                delegateCache::removeAll,
                (mapToAdd, mapToRemove ) -> delegateCache.updateAll(transformMap(mapToAdd, mappingFunction), mapToRemove)
        ));
    }
}
