package io.github.pellse.assembler.caching;

import io.github.pellse.assembler.caching.CacheContext.OneToManyCacheContext;
import io.github.pellse.assembler.caching.CacheFactory.CacheTransformer;

import java.util.Collection;
import java.util.Comparator;
import java.util.function.Function;

import static io.github.pellse.assembler.caching.MapperCacheFactory.mapper;
import static io.github.pellse.util.ObjectUtils.also;
import static java.util.stream.Collectors.toCollection;

public interface SortByCacheFactory {

    static <ID, EID, R, RC extends Collection<R>> CacheTransformer<ID, EID, R, RC, OneToManyCacheContext<ID, EID, R, RC>> sortBy(Comparator<R> comparator) {
        return cacheFactory -> sortBy(cacheFactory, comparator);
    }

    static <ID, EID, R, RC extends Collection<R>> CacheFactory<ID, EID, R, RC, OneToManyCacheContext<ID, EID, R, RC>> sortBy(CacheFactory<ID, EID, R, RC, OneToManyCacheContext<ID, EID, R, RC>> cacheFactory) {
        return sortBy(cacheFactory, (Comparator<R>) null);
    }

    static <ID, EID, R, RC extends Collection<R>> CacheFactory<ID, EID, R, RC, OneToManyCacheContext<ID, EID, R, RC>> sortBy(CacheFactory<ID, EID, R, RC, OneToManyCacheContext<ID, EID, R, RC>> cacheFactory, Comparator<R> comparator) {
        return sortBy(cacheFactory, cacheContext -> comparator != null ? comparator : cacheContext.ctx().idComparator());
    }

    private static <ID, EID, R, RC extends Collection<R>> CacheFactory<ID, EID, R, RC, OneToManyCacheContext<ID, EID, R, RC>> sortBy(CacheFactory<ID, EID, R, RC, OneToManyCacheContext<ID, EID, R, RC>> cacheFactory, Function<OneToManyCacheContext<ID, EID, R, RC>, Comparator<R>> comparatorProvider) {
        return mapper(cacheFactory,
                cacheContext ->
                        (id, coll) -> also(coll.stream()
                                .sorted(comparatorProvider.apply(cacheContext))
                                .collect(toCollection(cacheContext.ctx().collectionFactory())), System.out::println));
    }
}