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

import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import static io.github.pellse.assembler.caching.factory.MapperCacheFactory.mapper;

public interface SortByCacheFactory {

    static <ID, EID, R> CacheTransformer<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>> sortBy() {
        return cacheFactory -> sortBy(cacheFactory, (Comparator<R>) null);
    }

    static <ID, EID, R> CacheTransformer<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>> sortBy(Comparator<R> comparator) {
        return cacheFactory -> sortBy(cacheFactory, comparator);
    }

    static <ID, EID, R> CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>> sortBy(
            CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>> cacheFactory) {

        return sortBy(cacheFactory, (Comparator<R>) null);
    }

    static <ID, EID, R> CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>> sortBy(
            CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>> cacheFactory,
            Comparator<R> comparator) {

        return sortBy(cacheFactory, cacheContext -> comparator != null ? comparator : cacheContext.idComparator());
    }

    private static <ID, EID, R> CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>> sortBy(
            CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>> cacheFactory,
            Function<OneToManyCacheContext<ID, EID, R>, Comparator<R>> comparatorProvider) {

        return mapper(cacheFactory,
                cacheContext ->
                        (__, coll) -> coll.stream()
                                .sorted(comparatorProvider.apply(cacheContext))
                                .toList());
    }
}