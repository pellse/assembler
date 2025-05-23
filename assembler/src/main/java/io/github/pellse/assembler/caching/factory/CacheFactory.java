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

import io.github.pellse.assembler.RuleMapperContext;
import io.github.pellse.assembler.RuleMapperContext.OneToManyContext;
import io.github.pellse.assembler.RuleMapperContext.OneToOneContext;
import io.github.pellse.assembler.RuleMapperSource;
import io.github.pellse.assembler.caching.Cache;
import io.github.pellse.assembler.caching.Cache.FetchFunction;
import io.github.pellse.assembler.caching.merge.MergeFunction;
import io.github.pellse.assembler.caching.factory.CacheContext.OneToManyCacheContext;
import io.github.pellse.assembler.caching.factory.CacheContext.OneToOneCacheContext;
import io.github.pellse.assembler.caching.merge.MergeFunctionFactory;
import io.github.pellse.util.collection.CollectionUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.github.pellse.assembler.QueryUtils.buildQueryFunction;
import static io.github.pellse.assembler.RuleMapperSource.*;
import static io.github.pellse.assembler.caching.DefaultCache.cache;
import static io.github.pellse.assembler.caching.factory.CacheContext.OneToManyCacheContext.oneToManyCacheContext;
import static io.github.pellse.assembler.caching.factory.CacheContext.OneToOneCacheContext.oneToOneCacheContext;
import static io.github.pellse.assembler.caching.factory.DeferCacheFactory.defer;
import static io.github.pellse.assembler.caching.OneToManyCache.oneToManyCache;
import static io.github.pellse.assembler.caching.OneToOneCache.oneToOneCache;
import static io.github.pellse.assembler.caching.factory.SerializeCacheFactory.serialize;
import static io.github.pellse.assembler.caching.merge.MergeFunctionContext.mergeFunctionContext;
import static io.github.pellse.util.ObjectUtils.*;
import static io.github.pellse.util.collection.CollectionUtils.*;
import static java.util.Arrays.stream;
import static java.util.function.Predicate.not;
import static reactor.core.publisher.Flux.fromStream;
import static reactor.core.publisher.Mono.just;

@FunctionalInterface
public interface CacheFactory<ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> {

    Cache<ID, RRC> create(CTX context);

    class QueryFunctionException extends Exception {
        public QueryFunctionException(Throwable t) {
            super(null, t, true, false);
        }
    }

    @SafeVarargs
    static <T, K, ID, R> RuleMapperSource<T, K, ID, ID, R, R, OneToOneContext<T, K, ID, R>> cached(
            Function<CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>, CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>>... delegateCacheFactories) {

        return cached(cache(), delegateCacheFactories);
    }

    @SafeVarargs
    static <T, K, ID, R> RuleMapperSource<T, K, ID, ID, R, R, OneToOneContext<T, K, ID, R>> cached(
            MergeFunction<ID, R> mergeFunction,
            Function<CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>, CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>>... delegateCacheFactories) {

        return cached(cache(), mergeFunction, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, K, ID, R> RuleMapperSource<T, K, ID, ID, R, R, OneToOneContext<T, K, ID, R>> cached(
            CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>> cache,
            Function<CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>, CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>>... delegateCacheFactories) {

        return cached(emptySource(), cache, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, K, ID, R> RuleMapperSource<T, K, ID, ID, R, R, OneToOneContext<T, K, ID, R>> cached(
            CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>> cache,
            MergeFunction<ID, R> mergeFunction,
            Function<CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>, CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>>... delegateCacheFactories) {

        return cached(emptySource(), cache, mergeFunction, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, K, ID, R> RuleMapperSource<T, K, ID, ID, R, R, OneToOneContext<T, K, ID, R>> cached(
            Function<List<T>, Publisher<R>> queryFunction,
            Function<CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>, CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>>... delegateCacheFactories) {

        return cached(from(queryFunction), delegateCacheFactories);
    }

    @SafeVarargs
    static <T, K, ID, R> RuleMapperSource<T, K, ID, ID, R, R, OneToOneContext<T, K, ID, R>> cached(
            Function<List<T>, Publisher<R>> queryFunction,
            MergeFunction<ID, R> mergeFunction,
            Function<CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>, CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>>... delegateCacheFactories) {

        return cached(from(queryFunction), mergeFunction, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, K, ID, R> RuleMapperSource<T, K, ID, ID, R, R, OneToOneContext<T, K, ID, R>> cached(
            RuleMapperSource<T, K, ID, ID, R, R, OneToOneContext<T, K, ID, R>> ruleMapperSource,
            Function<CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>, CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>>... delegateCacheFactories) {

        return cached(ruleMapperSource, cache(), delegateCacheFactories);
    }

    @SafeVarargs
    static <T, K, ID, R> RuleMapperSource<T, K, ID, ID, R, R, OneToOneContext<T, K, ID, R>> cached(
            RuleMapperSource<T, K, ID, ID, R, R, OneToOneContext<T, K, ID, R>> ruleMapperSource,
            MergeFunction<ID, R> mergeFunction,
            Function<CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>, CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>>... delegateCacheFactories) {

        return cached(ruleMapperSource, cache(), mergeFunction, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, K, ID, R> RuleMapperSource<T, K, ID, ID, R, R, OneToOneContext<T, K, ID, R>> cached(
            Function<List<T>, Publisher<R>> queryFunction,
            CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>> cacheFactory,
            Function<CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>, CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>>... delegateCacheFactories) {

        return cached(from(queryFunction), cacheFactory, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, K, ID, R> RuleMapperSource<T, K, ID, ID, R, R, OneToOneContext<T, K, ID, R>> cached(
            Function<List<T>, Publisher<R>> queryFunction,
            CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>> cacheFactory,
            MergeFunction<ID, R> mergeFunction,
            Function<CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>, CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>>... delegateCacheFactories) {

        return cached(from(queryFunction), cacheFactory, mergeFunction, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, K, ID, R> RuleMapperSource<T, K, ID, ID, R, R, OneToOneContext<T, K, ID, R>> cached(
            RuleMapperSource<T, K, ID, ID, R, R, OneToOneContext<T, K, ID, R>> ruleMapperSource,
            CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>> cacheFactory,
            Function<CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>, CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>>... delegateCacheFactories) {

        return cached(ruleMapperSource, cacheFactory, null, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, K, ID, R> RuleMapperSource<T, K, ID, ID, R, R, OneToOneContext<T, K, ID, R>> cached(
            RuleMapperSource<T, K, ID, ID, R, R, OneToOneContext<T, K, ID, R>> ruleMapperSource,
            CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>> cacheFactory,
            MergeFunction<ID, R> mergeFunction,
            Function<CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>, CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>>... delegateCacheFactories) {

        final var wrappedCacheFactory = wrap(cacheFactory);

        return cached(
                ctx -> oneToOneCacheContext(ctx, mergeFunction),
                ruleMapperSource,
                cacheCtx -> oneToOneCache(cacheCtx, wrappedCacheFactory.create(cacheCtx)),
                delegateCacheFactories);
    }

    @SafeVarargs
    static <T, K, ID, EID, R> RuleMapperSource<T, K, ID, EID, R, List<R>, OneToManyContext<T, K, ID, EID, R>> cachedMany(
            Function<CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>>, CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>>>... delegateCacheFactories) {

        return cachedMany(cache(), delegateCacheFactories);
    }

    @SafeVarargs
    static <T, K, ID, EID, R> RuleMapperSource<T, K, ID, EID, R, List<R>, OneToManyContext<T, K, ID, EID, R>> cachedMany(
            MergeFunctionFactory<ID, EID, R> mergeFunctionFactory,
            Function<CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>>, CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>>>... delegateCacheFactories) {

        return cachedMany(cache(), mergeFunctionFactory, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, K, ID, EID, R> RuleMapperSource<T, K, ID, EID, R, List<R>, OneToManyContext<T, K, ID, EID, R>> cachedMany(
            CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>> cacheFactory,
            Function<CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>>, CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>>>... delegateCacheFactories) {

        return cachedMany(emptySource(), cacheFactory, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, K, ID, EID, R> RuleMapperSource<T, K, ID, EID, R, List<R>, OneToManyContext<T, K, ID, EID, R>> cachedMany(
            CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>> cacheFactory,
            MergeFunctionFactory<ID, EID, R> mergeFunctionFactory,
            Function<CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>>, CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>>>... delegateCacheFactories) {

        return cachedMany(emptySource(), cacheFactory, mergeFunctionFactory, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, K, ID, EID, R> RuleMapperSource<T, K, ID, EID, R, List<R>, OneToManyContext<T, K, ID, EID, R>> cachedMany(
            Function<List<T>, Publisher<R>> queryFunction,
            Function<CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>>, CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>>>... delegateCacheFactories) {

        return cachedMany(from(queryFunction), delegateCacheFactories);
    }

    @SafeVarargs
    static <T, K, ID, EID, R> RuleMapperSource<T, K, ID, EID, R, List<R>, OneToManyContext<T, K, ID, EID, R>> cachedMany(
            Function<List<T>, Publisher<R>> queryFunction,
            MergeFunctionFactory<ID, EID, R> mergeFunctionFactory,
            Function<CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>>, CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>>>... delegateCacheFactories) {

        return cachedMany(from(queryFunction), mergeFunctionFactory, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, K, ID, EID, R> RuleMapperSource<T, K, ID, EID, R, List<R>, OneToManyContext<T, K, ID, EID, R>> cachedMany(
            RuleMapperSource<T, K, ID, EID, R, List<R>, OneToManyContext<T, K, ID, EID, R>> ruleMapperSource,
            Function<CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>>, CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>>>... delegateCacheFactories) {

        return cachedMany(ruleMapperSource, cache(), delegateCacheFactories);
    }

    @SafeVarargs
    static <T, K, ID, EID, R> RuleMapperSource<T, K, ID, EID, R, List<R>, OneToManyContext<T, K, ID, EID, R>> cachedMany(
            RuleMapperSource<T, K, ID, EID, R, List<R>, OneToManyContext<T, K, ID, EID, R>> ruleMapperSource,
            MergeFunctionFactory<ID, EID, R> mergeFunctionFactory,
            Function<CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>>, CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>>>... delegateCacheFactories) {

        return cachedMany(ruleMapperSource, cache(), mergeFunctionFactory, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, K, ID, EID, R> RuleMapperSource<T, K, ID, EID, R, List<R>, OneToManyContext<T, K, ID, EID, R>> cachedMany(
            Function<List<T>, Publisher<R>> queryFunction,
            CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>> cacheFactory,
            Function<CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>>, CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>>>... delegateCacheFactories) {

        return cachedMany(from(queryFunction), cacheFactory, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, K, ID, EID, R> RuleMapperSource<T, K, ID, EID, R, List<R>, OneToManyContext<T, K, ID, EID, R>> cachedMany(
            Function<List<T>, Publisher<R>> queryFunction,
            CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>> cacheFactory,
            MergeFunctionFactory<ID, EID, R> mergeFunctionFactory,
            Function<CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>>, CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>>>... delegateCacheFactories) {

        return cachedMany(from(queryFunction), cacheFactory, mergeFunctionFactory, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, K, ID, EID, R> RuleMapperSource<T, K, ID, EID, R, List<R>, OneToManyContext<T, K, ID, EID, R>> cachedMany(
            RuleMapperSource<T, K, ID, EID, R, List<R>, OneToManyContext<T, K, ID, EID, R>> ruleMapperSource,
            CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>> cacheFactory,
            Function<CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>>, CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>>>... delegateCacheFactories) {

        return cachedMany(ruleMapperSource, cacheFactory, null, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, K, ID, EID, R> RuleMapperSource<T, K, ID, EID, R, List<R>, OneToManyContext<T, K, ID, EID, R>> cachedMany(
            RuleMapperSource<T, K, ID, EID, R, List<R>, OneToManyContext<T, K, ID, EID, R>> ruleMapperSource,
            CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>> cacheFactory,
            MergeFunctionFactory<ID, EID, R> mergeFunctionFactory,
            Function<CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>>, CacheFactory<ID, R, List<R>, OneToManyCacheContext<ID, EID, R>>>... delegateCacheFactories) {

        final var wrappedCacheFactory = wrap(cacheFactory);

        return cached(
                ctx -> oneToManyCacheContext(ctx, mergeFunctionFactory != null ? mergeFunctionFactory.create(mergeFunctionContext(ctx)) : null),
                ruleMapperSource,
                cacheCtx -> oneToManyCache(cacheCtx, wrappedCacheFactory.create(cacheCtx)),
                delegateCacheFactories);
    }

    static <ID, RRC> Function<Map<ID, RRC>, Mono<?>> toMono(Consumer<Map<ID, RRC>> consumer) {
        return map -> just(also(map, consumer));
    }

    @SafeVarargs
    private static <T, K, ID, EID, R, RRC, CTX extends RuleMapperContext<T, K, ID, EID, R, RRC>, CACHE_CTX extends CacheContext<ID, R, RRC, CACHE_CTX>> RuleMapperSource<T, K, ID, EID, R, RRC, CTX> cached(
            Function<CTX, CACHE_CTX> cacheContextProvider,
            RuleMapperSource<T, K, ID, EID, R, RRC, CTX> ruleMapperSource,
            CacheFactory<ID, R, RRC, CACHE_CTX> cacheFactory,
            Function<CacheFactory<ID, R, RRC, CACHE_CTX>, CacheFactory<ID, R, RRC, CACHE_CTX>>... delegateCacheFactories) {

        final var isEmptySource = isEmptySource(ruleMapperSource);

        return ruleContext -> {
            final var queryFunction = nullToEmptySource(ruleMapperSource).apply(ruleContext);
            final var cacheQueryFunction = buildQueryFunction(queryFunction, ruleContext);

            final var cache = delegate(cacheFactory, delegateCacheFactories)
                    .create(cacheContextProvider.apply(ruleContext));

            return entities -> {
                final var ids = ids(entities, ruleContext);
                final FetchFunction<ID, RRC> fetchFunction = idList -> buildFetchFunction(entities, cacheQueryFunction, ruleContext).apply(idList);

                return (isEmptySource ? cache.getAll(ids) : cache.computeAll(ids, fetchFunction))
                        .filter(CollectionUtils::isNotEmpty)
                        .flatMapMany(map -> fromStream(ruleContext.streamFlattener().apply(map.values().stream())))
                        .onErrorResume(not(QueryFunctionException.class::isInstance), __ -> queryFunction.apply(entities))
                        .onErrorMap(QueryFunctionException.class, Throwable::getCause);
            };
        };
    }

    private static <T, K, ID, EID, R, RRC> List<ID> ids(List<T> entities, RuleMapperContext<T, K, ID, EID, R, RRC> ruleContext) {
        return transform(entities, ruleContext.outerIdResolver());
    }

    private static <T, K, ID, EID, R, RRC, CTX extends RuleMapperContext<T, K, ID, EID, R, RRC>> FetchFunction<ID, RRC> buildFetchFunction(
            List<T> entities,
            Function<Iterable<T>, Mono<Map<ID, RRC>>> queryFunction,
            CTX ruleContext) {

        return ids -> {
            if (isEmpty(ids)) {
                return just(Map.<ID, RRC>of());
            }

            final var idSet = new HashSet<>(asCollection(ids));

            final var entitiesToQuery = toStream(entities)
                    .filter(e -> idSet.contains(ruleContext.outerIdResolver().apply(e)))
                    .toList();

            return queryFunction.apply(entitiesToQuery)
                    .map(queryResultsMap -> buildCacheFragment(ids, queryResultsMap, ruleContext))
                    .onErrorMap(QueryFunctionException::new);
        };
    }

    private static <ID, R, RRC, CACHE_CTX extends CacheContext<ID, R, RRC, CACHE_CTX>> CacheFactory<ID, R, RRC, CACHE_CTX> wrap(CacheFactory<ID, R, RRC, CACHE_CTX> cacheFactory) {
        return defer(serialize(cacheFactory));
    }

    @SafeVarargs
    private static <ID, R, RRC, CACHE_CTX extends CacheContext<ID, R, RRC, CACHE_CTX>> CacheFactory<ID, R, RRC, CACHE_CTX> delegate(
            CacheFactory<ID, R, RRC, CACHE_CTX> cacheFactory,
            Function<CacheFactory<ID, R, RRC, CACHE_CTX>, CacheFactory<ID, R, RRC, CACHE_CTX>>... delegateCacheFactories) {

        return stream(delegateCacheFactories)
                .reduce(cacheFactory,
                        (previousCacheFactory, delegateCacheFactoryFunction) -> delegateCacheFactoryFunction.apply(previousCacheFactory),
                        (previousCacheFactory, decoratedCacheFactory) -> decoratedCacheFactory);
    }

    private static <T, K, ID, EID, R, RRC> Map<ID, RRC> buildCacheFragment(
            Iterable<? extends ID> ids,
            Map<ID, RRC> queryResultsMap,
            RuleMapperContext<T, K, ID, EID, R, RRC> ctx) {

        return newMap(queryResultsMap, map ->
                diff(ids, map.keySet())
                        .forEach(id -> ifNotNull(ctx.defaultResultProvider().apply(id), value -> map.put(id, value))));
    }
}
