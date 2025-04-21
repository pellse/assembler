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
import io.github.pellse.util.function.Function3;

import java.util.*;
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

    Function3<ID, RRC, RRC, RRC> mergeFunction();

    CacheTransformer<ID, R, RRC, CTX> cacheTransformer();

    default BiFunction<Map<ID, RRC>, Map<ID, RRC>, Map<ID, RRC>> mapMerger() {
        return (existingMap, newMap) -> mergeMaps(existingMap, newMap, mergeFunction());
    }

    record OneToOneCacheContext<ID, R>(
            IntFunction<Collector<R, ?, Map<ID, R>>> mapCollector,
            Function3<ID, R, R, R> mergeFunction,
            CacheTransformer<ID, R, R, OneToOneCacheContext<ID, R>> cacheTransformer) implements CacheContext<ID, R, R, OneToOneCacheContext<ID, R>> {

        static<ID, R> OneToOneCacheContext<ID, R> oneToOneCacheContext(OneToOneContext<?, ?, ?, ID, R> ctx) {
            return oneToOneCacheContext(ctx, (k, r1, r2) -> r2 != null ? r2 : r1);
        }

        static<ID, R> OneToOneCacheContext<ID, R> oneToOneCacheContext(OneToOneContext<?, ?, ?, ID, R> ctx, Function3<ID, R, R, R> mergeFunction) {
            return new OneToOneCacheContext<>(ctx.mapCollector(),
                    mergeFunction,
                   concurrent());
        }
    }

    record OneToManyCacheContext<ID, EID, R, RC extends Collection<R>> (
            Function<R, EID> idResolver,
            IntFunction<Collector<R, ?, Map<ID, RC>>> mapCollector,
            Comparator<R> idComparator,
            Supplier<RC> collectionFactory,
            Class<RC> collectionType,
            Function3<ID, RC, RC, RC> mergeFunction,
            CacheTransformer<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>> cacheTransformer) implements CacheContext<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>> {

        static <ID, EID, R, RC extends Collection<R>> OneToManyCacheContext<ID, EID, R, RC> oneToManyCacheContext(OneToManyContext<?, ?, ?, ID, EID, R, RC> ctx) {
            return oneToManyCacheContext(ctx, removeDuplicate(ctx));
        }

        static <ID, EID, R, RC extends Collection<R>> OneToManyCacheContext<ID, EID, R, RC> oneToManyCacheContext(
                OneToManyContext<?, ?, ?, ID, EID, R, RC> ctx,
                Function3<ID, RC, RC, RC> mergeFunction) {

            return new OneToManyCacheContext<>(ctx.idResolver(),
                    ctx.mapCollector(),
                    ctx.idComparator(),
                    ctx.collectionFactory(),
                    ctx.collectionType(),
                    (id, coll1, coll2) ->
                            isNotEmpty(coll1) || isNotEmpty(coll2) ? mergeFunction.apply(id, convert(coll1, ctx), convert(coll2, ctx)) : convert(List.of(), ctx),
                    concurrent());
        }

        private RC convert(Collection<R> collection) {
            return convert(collection, collectionType, collectionFactory);
        }

        private static <ID, EID, R, RC extends Collection<R>> Function3<ID, RC, RC, RC> removeDuplicate(OneToManyContext<?, ?, ?, ID, EID, R, RC> ctx) {
            return (id, coll1, coll2) -> removeDuplicates(concat(coll1, coll2), ctx.idResolver(), rc -> convert(rc, ctx));
        }

        private static <ID, EID, R, RC extends Collection<R>> RC convert(Collection<R> collection, OneToManyContext<?, ?, ?, ID, EID, R, RC> ctx) {
            return convert(collection, ctx.collectionType(), ctx.collectionFactory());
        }

        @SuppressWarnings("unchecked")
        private static <R, RC extends Collection<R>> RC convert(Collection<R> collection, Class<RC> collectionType, Supplier<RC> collectionFactory) {
            return collectionType.isInstance(collection) ? (RC) collection : translate(collection, collectionFactory);
        }
    }
}
