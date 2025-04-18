/*
 * Copyright 2024 Sebastien Pelletier
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.pellse.assembler.caching.factory;

import io.github.pellse.assembler.caching.factory.CacheContext.OneToManyCacheContext;
import io.github.pellse.assembler.caching.factory.CacheFactory.CacheTransformer;

import java.util.Collection;
import java.util.Comparator;
import java.util.function.Function;

import static io.github.pellse.assembler.caching.factory.MapperCacheFactory.mapper;
import static java.util.stream.Collectors.toCollection;

public interface SortByCacheFactory {

    static <ID, EID, R, RC extends Collection<R>> CacheTransformer<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC, R, RC>> sortBy() {
        return cacheFactory -> sortBy(cacheFactory, (Comparator<R>) null);
    }

    static <ID, EID, R, RC extends Collection<R>> CacheTransformer<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC, R, RC>> sortBy(Comparator<R> comparator) {
        return cacheFactory -> sortBy(cacheFactory, comparator);
    }

    static <ID, EID, R, RC extends Collection<R>> CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC, R, RC>> sortBy(
            CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC, R, RC>> cacheFactory) {

        return sortBy(cacheFactory, (Comparator<R>) null);
    }

    static <ID, EID, R, RC extends Collection<R>> CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC, R, RC>> sortBy(
            CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC, R, RC>> cacheFactory,
            Comparator<R> comparator) {

        return sortBy(cacheFactory, cacheContext -> comparator != null ? comparator : cacheContext.idComparator());
    }

    private static <ID, EID, R, RC extends Collection<R>> CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC, R, RC>> sortBy(
            CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC, R, RC>> cacheFactory,
            Function<OneToManyCacheContext<ID, EID, R, RC, R, RC>, Comparator<R>> comparatorProvider) {

        return mapper(cacheFactory,
                cacheContext ->
                        (__, coll) -> coll.stream()
                                .sorted(comparatorProvider.apply(cacheContext))
                                .collect(toCollection(cacheContext.collectionFactory())));
    }
}