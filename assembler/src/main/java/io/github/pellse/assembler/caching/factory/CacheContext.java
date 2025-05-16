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
import io.github.pellse.assembler.caching.merge.MergeFunction;
import io.github.pellse.assembler.caching.merge.MergeFunctions;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collector;

import static io.github.pellse.assembler.caching.factory.ConcurrentCacheFactory.concurrent;
import static io.github.pellse.util.collection.CollectionUtils.*;
import static java.util.Objects.requireNonNullElse;

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

        public OneToOneCacheContext {
                mergeFunction = requireNonNullElse(mergeFunction, MergeFunctions::replace);
        }

        static <ID, R> OneToOneCacheContext<ID, R> oneToOneCacheContext(OneToOneContext<?, ?, ID, R> ctx) {
            return oneToOneCacheContext(ctx, null);
        }

        static <ID, R> OneToOneCacheContext<ID, R> oneToOneCacheContext(OneToOneContext<?, ?, ID, R> ctx, MergeFunction<ID, R> mergeFunction) {
            return new OneToOneCacheContext<>(ctx.mapCollector(), mergeFunction, concurrent());
        }
    }

    record OneToManyCacheContext<ID, EID, R>(
            Function<R, EID> idResolver,
            IntFunction<Collector<R, ?, Map<ID, List<R>>>> mapCollector,
            Comparator<R> idComparator,
            MergeFunction<ID, List<R>> mergeFunction,
            CacheTransformer<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>> cacheTransformer) implements CacheContext<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>> {

        public OneToManyCacheContext {
            final var mf = requireNonNullElse(mergeFunction, (id, coll1, coll2) -> removeDuplicates(concat(coll1, coll2), idResolver()));
            mergeFunction = (id, coll1, coll2) -> isNotEmpty(coll1) || isNotEmpty(coll2) ? mf.apply(id, coll1, coll2) : List.of();
        }

        static <ID, EID, R> OneToManyCacheContext<ID, EID, R> oneToManyCacheContext(OneToManyContext<?, ?, ID, EID, R> ctx) {
            return oneToManyCacheContext(ctx, null);
        }

        static <ID, EID, R> OneToManyCacheContext<ID, EID, R> oneToManyCacheContext(
                OneToManyContext<?, ?, ID, EID, R> ctx,
                MergeFunction<ID, List<R>> mergeFunction) {

            return new OneToManyCacheContext<>(ctx.idResolver(),
                    ctx.mapCollector(),
                    ctx.idComparator(),
                    mergeFunction,
                    concurrent());
        }
    }
}
