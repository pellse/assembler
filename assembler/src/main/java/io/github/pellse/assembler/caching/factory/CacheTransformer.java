package io.github.pellse.assembler.caching.factory;

import java.util.List;
import java.util.function.Function;

@FunctionalInterface
public interface CacheTransformer<ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> extends Function<CacheFactory<ID, R, RRC, CTX>, CacheFactory<ID, R, RRC, CTX>> {

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheTransformer<ID, R, RRC, CTX> defaultCacheTransformer() {
        return cf -> cf;
    }

    static <ID, R> CacheTransformer<ID, R, R, CacheContext.OneToOneCacheContext<ID, R>> oneToOneCacheTransformer(CacheTransformer<ID, R, R, CacheContext.OneToOneCacheContext<ID, R>> cacheTransformer) {
        return cacheTransformer;
    }

    static <ID, EID, R> CacheTransformer<ID, R, List<R>, CacheContext.OneToManyCacheContext<ID, EID, R>> oneToManyCacheTransformer(CacheTransformer<ID, R, List<R>, CacheContext.OneToManyCacheContext<ID, EID, R>> cacheTransformer) {
        return cacheTransformer;
    }

    static <ID, R> CacheTransformer<ID, R, R, CacheContext.OneToOneCacheContext<ID, R>> resolve(
            CacheTransformer<ID, R, R, CacheContext.OneToOneCacheContext<ID, R>> cacheTransformer,
            @SuppressWarnings("unused") Class<ID> idClass) {

        return cacheTransformer;
    }

    static <ID, EID, R> CacheTransformer<ID, R, List<R>, CacheContext.OneToManyCacheContext<ID, EID, R>> resolve(
            CacheTransformer<ID, R, List<R>, CacheContext.OneToManyCacheContext<ID, EID, R>> cacheTransformer,
            @SuppressWarnings("unused") Class<ID> idClass,
            @SuppressWarnings("unused") Class<EID> elementIdClass) {

        return cacheTransformer;
    }
}
