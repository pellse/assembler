package io.github.pellse.assembler.caching.factory;

import io.github.pellse.assembler.caching.factory.CacheContext.OneToManyCacheContext;
import io.github.pellse.assembler.caching.factory.CacheContext.OneToOneCacheContext;

import java.util.List;
import java.util.function.Function;

@FunctionalInterface
public interface CacheTransformer<ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> extends Function<CacheFactory<ID, R, RRC, CTX>, CacheFactory<ID, R, RRC, CTX>> {

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheTransformer<ID, R, RRC, CTX> defaultCacheTransformer() {
        return cf -> cf;
    }

    static <ID, R> CacheTransformer<ID, R, R, OneToOneCacheContext<ID, R>> oneToOneCacheTransformer(CacheTransformer<ID, R, R, OneToOneCacheContext<ID, R>> cacheTransformer) {
        return cacheTransformer;
    }

    static <ID, EID, R> CacheTransformer<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>> oneToManyCacheTransformer(CacheTransformer<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>> cacheTransformer) {
        return cacheTransformer;
    }

    static <ID, R> CacheTransformer<ID, R, R, OneToOneCacheContext<ID, R>> resolve(
            CacheTransformer<ID, R, R, OneToOneCacheContext<ID, R>> cacheTransformer,
            @SuppressWarnings("unused") Class<ID> idClass) {

        return cacheTransformer;
    }

    static <ID, EID, R> CacheTransformer<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>> resolve(
            CacheTransformer<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>> cacheTransformer,
            @SuppressWarnings("unused") Class<ID> idClass,
            @SuppressWarnings("unused") Class<EID> elementIdClass) {

        return cacheTransformer;
    }

    static <ID> ElementIDResolver<ID> withIDType(@SuppressWarnings("unused") Class<ID> idType) {
        return new ElementIDResolver<>() {

            @Override
            public <R> CacheTransformer<ID, R, R, OneToOneCacheContext<ID, R>> resolve(CacheTransformer<ID, R, R, OneToOneCacheContext<ID, R>> cacheTransformer) {
                return CacheTransformer.resolve(cacheTransformer, idType);
            }

            @Override
            public <EID> OneToManyCacheTransformerResolver<ID, EID> andElementIDType(Class<EID> elementIdType) {
                return new OneToManyCacheTransformerResolver<>() {

                    @Override
                    public <R> CacheTransformer<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>> resolve(CacheTransformer<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>> cacheTransformer) {
                        return CacheTransformer.resolve(cacheTransformer, idType, elementIdType);
                    }
                };
            }
        };
    }

    @FunctionalInterface
    interface ElementIDResolver<ID> extends OneToOneCacheTransformerResolver<ID> {
       <EID> OneToManyCacheTransformerResolver<ID, EID> andElementIDType(@SuppressWarnings("unused") Class<EID> elementIdType);
    }

    @FunctionalInterface
    interface OneToOneCacheTransformerResolver<ID>  {
        <R> CacheTransformer<ID, R, R, OneToOneCacheContext<ID, R>> resolve(CacheTransformer<ID, R, R, OneToOneCacheContext<ID, R>> cacheTransformer);
    }

    @FunctionalInterface
    interface OneToManyCacheTransformerResolver<ID, EID>  {
        <R> CacheTransformer<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>> resolve(CacheTransformer<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>> cacheTransformer);
    }
}
