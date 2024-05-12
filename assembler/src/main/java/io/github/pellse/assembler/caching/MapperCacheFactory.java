package io.github.pellse.assembler.caching;

import io.github.pellse.assembler.caching.CacheContext.OneToManyCacheContext;
import io.github.pellse.assembler.caching.CacheContext.OneToOneCacheContext;
import io.github.pellse.assembler.caching.CacheFactory.CacheTransformer;

import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.github.pellse.assembler.caching.Cache.adapterCache;
import static io.github.pellse.util.ObjectUtils.then;
import static io.github.pellse.util.collection.CollectionUtils.transformMap;

public interface MapperCacheFactory {

    static <ID, R> CacheTransformer<ID, ID, R, R, OneToOneCacheContext<ID, R>> oneToOneMapper(BiFunction<ID, R, R> mappingFunction) {
        return mapper(__ -> mappingFunction);
    }

    static <ID, EID, R, RC extends Collection<R>> CacheTransformer<ID, EID, R, RC, OneToManyCacheContext<ID, EID, R, RC>> oneToManyMapper(BiFunction<ID, RC, ? extends Collection<R>> mappingFunction) {
        return mapper(context -> (id, coll) -> context.convert(mappingFunction.apply(id, coll)));
    }

    static <ID, EID, R, RRC, CTX extends CacheContext<ID, EID, R, RRC, CTX>> CacheTransformer<ID, EID, R, RRC, CTX> mapper(Function<CTX, BiFunction<ID, RRC, RRC>> contextMappingFunction) {
        return cacheFactory -> context -> then(cacheFactory.create(context), delegateCache -> adapterCache(
                delegateCache::getAll,
                delegateCache::computeAll,
                map -> delegateCache.putAll(transformMap(map, contextMappingFunction.apply(context))),
                delegateCache::removeAll,
                (mapToAdd, mapToRemove ) -> delegateCache.updateAll(transformMap(mapToAdd, contextMappingFunction.apply(context)), mapToRemove)
        ));
    }
}