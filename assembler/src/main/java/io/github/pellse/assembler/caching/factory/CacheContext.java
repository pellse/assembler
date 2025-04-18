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

import io.github.pellse.assembler.RuleMapperContext.OneToManyContext;
import io.github.pellse.assembler.RuleMapperContext.OneToOneContext;
import io.github.pellse.assembler.caching.factory.CacheFactory.CacheTransformer;
import io.github.pellse.util.collection.CollectionUtils;
import io.github.pellse.util.function.Function3;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static io.github.pellse.assembler.caching.factory.ConcurrentCacheFactory.concurrent;
import static io.github.pellse.util.collection.CollectionUtils.*;
import static io.github.pellse.util.collection.CollectionUtils.mergeMaps;

public sealed interface CacheContext<ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> {

    IntFunction<Collector<R, ?, Map<ID, RRC>>> mapCollector();

    BiFunction<Map<ID, RRC>, Map<ID, RRC>, Map<ID, RRC>> mapMerger();

    CacheTransformer<ID, R, RRC, CTX> cacheTransformer();

    record OneToOneCacheContext<ID, R, U>(
            IntFunction<Collector<R, ?, Map<ID, R>>> mapCollector,
            Function3<ID, R, U, R> mergeFunction,
            CacheTransformer<ID, R, R, OneToOneCacheContext<ID, R, U>> cacheTransformer) implements CacheContext<ID, R, R, OneToOneCacheContext<ID, R, U>> {

        static<ID, R> OneToOneCacheContext<ID, R, R> oneToOneCacheContext(OneToOneContext<?, ?, ?, ID, R> ctx) {
            return oneToOneCacheContext(ctx, (k, r1, r2) -> r2 != null ? r2 : r1);
        }

        static<ID, R> OneToOneCacheContext<ID, R, R> oneToOneCacheContext(OneToOneContext<?, ?, ?, ID, R> ctx, Function3<ID, R, R, R> mergeFunction) {
            return new OneToOneCacheContext<>(ctx.mapCollector(),
                    mergeFunction,
                    concurrent());
        }

        public BiFunction<Map<ID, R>, Map<ID, U>, Map<ID, R>> mapMerger(Function3<ID, R, U, R> mergeFunction) {
            return (existingMap, newMap) -> mergeMaps(existingMap, newMap, mergeFunction);
        }

        public BiFunction<Map<ID, R>, Map<ID, R>, Map<ID, R>> mapMerger() {
            return CollectionUtils::mergeMaps;
        }
    }

    record OneToManyCacheContext<ID, EID, R, RC extends Collection<R>, U, UC extends Collection<U>> (
            Function<R, EID> idResolver,
            IntFunction<Collector<R, ?, Map<ID, RC>>> mapCollector,
            Comparator<R> idComparator,
            Supplier<RC> collectionFactory,
            Class<RC> collectionType,
            Function3<ID, RC, UC, RC> mergeFunction,
            CacheTransformer<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC, U, UC>> cacheTransformer) implements CacheContext<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC, U, UC>> {

        static <ID, EID, R, RC extends Collection<R>> OneToManyCacheContext<ID, EID, R, RC, R, RC> oneToManyCacheContext(OneToManyContext<?, ?, ?, ID, EID, R, RC> ctx) {

            return oneToManyCacheContext(
                    ctx,
                    (k, coll1, coll2) -> removeDuplicates(concat(coll1, coll2), ctx.idResolver(), rc -> convert(rc, ctx.collectionType(), ctx.collectionFactory())));
        }

        static <ID, EID, R, RC extends Collection<R>, U, UC extends Collection<U>> OneToManyCacheContext<ID, EID, R, RC, U, UC> oneToManyCacheContext(
                OneToManyContext<?, ?, ?, ID, EID, R, RC> ctx,
                Function3<ID, RC, UC, RC> mergeFunction) {

            return new OneToManyCacheContext<>(ctx.idResolver(),
                    ctx.mapCollector(),
                    ctx.idComparator(),
                    ctx.collectionFactory(),
                    ctx.collectionType(),
                    mergeFunction,
                    concurrent());
        }

        BiFunction<Map<ID, RC>, Map<ID, Collection<U>>, Map<ID, RC>> mapMerger(Function3<ID, RC, Collection<U>, RC> mergeFunction) {

            Function3<ID, RC, Collection<U>, RC> mappingFunction = (id, coll1, coll2) ->
                    isNotEmpty(coll1) || isNotEmpty(coll2) ? mergeFunction.apply(id, convert(coll1), asCollection(coll2)) : convert(List.of());

            return (existingMap, newMap) -> mergeMaps(existingMap, newMap, mappingFunction);
        }

        public BiFunction<Map<ID, RC>, Map<ID, RC>, Map<ID, RC>> mapMerger() {
            return (existingMap, newMap) -> mergeMaps(existingMap, newMap, idResolver(), this::convert);
        }

        @SuppressWarnings("unchecked")
        private RC convert(Collection<R> collection) {
            return collectionType().isInstance(collection) ? (RC) collection : translate(collection, collectionFactory());
        }

        @SuppressWarnings("unchecked")
        public static <R, RC extends Collection<R>> RC convert(Collection<R> collection, Class<RC> collectionType, Supplier<RC> collectionFactory) {
            return collectionType.isInstance(collection) ? (RC) collection : translate(collection, collectionFactory);
        }
    }
}
