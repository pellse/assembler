package io.github.pellse.assembler.caching;

import io.github.pellse.assembler.caching.CacheContext.OneToManyCacheContext;
import io.github.pellse.assembler.caching.CacheFactory.CacheTransformer;

import java.util.Collection;
import java.util.Comparator;

import static io.github.pellse.assembler.caching.MapperCacheFactory.mapper;
import static java.util.stream.Collectors.toCollection;

public interface SortedCacheFactory {

    static <ID, EID, R, RC extends Collection<R>> CacheTransformer<ID, EID, R, RC, OneToManyCacheContext<ID, EID, R, RC>> sorted(Comparator<R> comparator) {
        return cacheFactory -> sorted(cacheFactory, comparator);
    }

    static <ID, EID, R, RC extends Collection<R>> CacheFactory<ID, EID, R, RC, OneToManyCacheContext<ID, EID, R, RC>> sorted(CacheFactory<ID, EID, R, RC, OneToManyCacheContext<ID, EID, R, RC>> cacheFactory, Comparator<R> comparator) {
        return mapper(cacheFactory,
                cacheContext ->
                        (id, coll) -> coll.stream()
                                .sorted(comparator)
                                .collect(toCollection(cacheContext.ctx().collectionFactory())));
    }
}