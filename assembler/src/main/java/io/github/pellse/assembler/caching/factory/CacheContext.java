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
import io.github.pellse.assembler.caching.Cache.MergeFunction;
import io.github.pellse.assembler.caching.factory.CacheFactory.CacheTransformer;
import io.github.pellse.util.collection.CollectionUtils;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static io.github.pellse.assembler.caching.factory.ConcurrentCacheFactory.concurrent;
import static io.github.pellse.util.collection.CollectionUtils.*;

public sealed interface CacheContext<ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> {

    IntFunction<Collector<R, ?, Map<ID, RRC>>> mapCollector();

    MergeFunction<ID, RRC> mergeFunction();

    CacheTransformer<ID, R, RRC, CTX> cacheTransformer();

    default BiFunction<Map<ID, RRC>, Map<ID, RRC>, Map<ID, RRC>> mapMerger() {
        return (existingMap, newMap) -> mergeMaps(existingMap, newMap, mergeFunction());
    }

    record OneToOneCacheContext<ID, R>(
            IntFunction<Collector<R, ?, Map<ID, R>>> mapCollector,
            MergeFunction<ID, R> mergeFunction,
            CacheTransformer<ID, R, R, OneToOneCacheContext<ID, R>> cacheTransformer) implements CacheContext<ID, R, R, OneToOneCacheContext<ID, R>> {

        static <ID, R> OneToOneCacheContext<ID, R> oneToOneCacheContext(OneToOneContext<?, ?, ?, ID, R> ctx) {
            return oneToOneCacheContext(ctx, (k, r1, r2) -> r2 != null ? r2 : r1);
        }

        static <ID, R> OneToOneCacheContext<ID, R> oneToOneCacheContext(OneToOneContext<?, ?, ?, ID, R> ctx, MergeFunction<ID, R> mergeFunction) {
            return new OneToOneCacheContext<>(ctx.mapCollector(),
                    mergeFunction,
                    concurrent());
        }
    }

    record OneToManyCacheContext<ID, EID, R, RC extends Collection<R>>(
            Function<R, EID> idResolver,
            IntFunction<Collector<R, ?, Map<ID, RC>>> mapCollector,
            Comparator<R> idComparator,
            Supplier<RC> collectionFactory,
            Class<RC> collectionType,
            MergeFunction<ID, RC> mergeFunction,
            CacheTransformer<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>> cacheTransformer) implements CacheContext<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>> {

        static <ID, EID, R, RC extends Collection<R>> OneToManyCacheContext<ID, EID, R, RC> oneToManyCacheContext(OneToManyContext<?, ?, ?, ID, EID, R, RC> ctx) {
            return oneToManyCacheContext(ctx, removeDuplicate(ctx));
        }

        static <ID, EID, R, RC extends Collection<R>> OneToManyCacheContext<ID, EID, R, RC> oneToManyCacheContext(
                OneToManyContext<?, ?, ?, ID, EID, R, RC> ctx,
                MergeFunction<ID, RC> mergeFunction) {

            return new OneToManyCacheContext<>(ctx.idResolver(),
                    ctx.mapCollector(),
                    ctx.idComparator(),
                    ctx.collectionFactory(),
                    ctx.collectionType(),
                    (id, coll1, coll2) -> isNotEmpty(coll1) || isNotEmpty(coll2) ? mergeFunction.apply(id, convert(coll1, ctx), convert(coll2, ctx)) : convert(List.of(), ctx),
                    concurrent());
        }

        public static <ID, EID, R, RC extends Collection<R>> MergeFunction<ID, RC> removeDuplicate(OneToManyContext<?, ?, ?, ID, EID, R, RC> ctx) {
            return (id, coll1, coll2) -> removeDuplicates(concat(coll1, coll2), ctx.idResolver(), rc -> convert(rc, ctx));
        }

        private static <ID, EID, R, RC extends Collection<R>> RC convert(Collection<R> collection, OneToManyContext<?, ?, ?, ID, EID, R, RC> ctx) {
            return CollectionUtils.convert(collection, ctx.collectionType(), ctx.collectionFactory());
        }
    }
}
