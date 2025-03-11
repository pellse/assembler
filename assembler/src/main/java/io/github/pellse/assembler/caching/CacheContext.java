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

package io.github.pellse.assembler.caching;

import io.github.pellse.assembler.RuleMapperContext.OneToManyContext;
import io.github.pellse.assembler.RuleMapperContext.OneToOneContext;
import io.github.pellse.assembler.caching.CacheFactory.CacheTransformer;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static io.github.pellse.assembler.caching.CacheFactory.CacheTransformer.defaultCacheTransformer;
import static io.github.pellse.assembler.caching.ConcurrentCacheFactory.concurrent;

public sealed interface CacheContext<ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> {

    IntFunction<Collector<R, ?, Map<ID, RRC>>> mapCollector();

    BiFunction<Map<ID, RRC>, Map<ID, RRC>, Map<ID, RRC>> mapMerger();

    CacheTransformer<ID, R, RRC, CTX> cacheTransformer();

    record OneToOneCacheContext<ID, R>(
            IntFunction<Collector<R, ?, Map<ID, R>>> mapCollector,
            BiFunction<Map<ID, R>, Map<ID, R>, Map<ID, R>> mapMerger,
            CacheTransformer<ID, R, R, OneToOneCacheContext<ID, R>> cacheTransformer) implements CacheContext<ID, R, R, OneToOneCacheContext<ID, R>> {

        OneToOneCacheContext(OneToOneContext<?, ?, ?, ID, R> ctx) {
            this(ctx.mapCollector(),
                    ctx.mapMerger(),
                    defaultCacheTransformer());
        }
    }

    record OneToManyCacheContext<ID, EID, R, RC extends Collection<R>>(
            Function<R, EID> idResolver,
            IntFunction<Collector<R, ?, Map<ID, RC>>> mapCollector,
            BiFunction<Map<ID, RC>, Map<ID, RC>, Map<ID, RC>> mapMerger,
            Comparator<R> idComparator,
            Supplier<RC> collectionFactory,
            CacheTransformer<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>> cacheTransformer) implements CacheContext<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>> {

        OneToManyCacheContext(OneToManyContext<?, ?, ?, ID, EID, R, RC> ctx) {
            this(ctx.idResolver(),
                    ctx.mapCollector(),
                    ctx.mapMerger(),
                    ctx.idComparator(),
                    ctx.collectionFactory(),
                    concurrent());
        }
    }
}
