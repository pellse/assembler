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

import io.github.pellse.assembler.RuleMapperContext;
import io.github.pellse.assembler.RuleMapperContext.OneToManyContext;
import io.github.pellse.assembler.RuleMapperContext.OneToOneContext;
import io.github.pellse.assembler.RuleMapperSource;
import io.github.pellse.assembler.caching.Cache.FetchFunction;
import io.github.pellse.assembler.caching.CacheContext.OneToManyCacheContext;
import io.github.pellse.assembler.caching.CacheContext.OneToOneCacheContext;
import io.github.pellse.util.collection.CollectionUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Empty;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.github.pellse.assembler.QueryUtils.buildQueryFunction;
import static io.github.pellse.assembler.RuleMapperSource.*;
import static io.github.pellse.assembler.caching.Cache.*;
import static io.github.pellse.assembler.caching.DeferCacheFactory.defer;
import static io.github.pellse.assembler.caching.SortByCacheFactory.sortBy;
import static io.github.pellse.util.ObjectUtils.*;
import static io.github.pellse.util.collection.CollectionUtils.*;
import static io.github.pellse.util.reactive.ReactiveUtils.createSinkMap;
import static io.github.pellse.util.reactive.ReactiveUtils.resolve;
import static java.util.Arrays.stream;
import static java.util.Optional.ofNullable;
import static java.util.function.Predicate.not;
import static reactor.core.publisher.Flux.fromStream;
import static reactor.core.publisher.Mono.just;

@FunctionalInterface
public interface CacheFactory<ID, R, RRC, CTX extends CacheContext<ID, R, RRC>> {

    Cache<ID, RRC> create(CTX context);

    @FunctionalInterface
    interface CacheTransformer<ID, R, RRC, CTX extends CacheContext<ID, R, RRC>> extends Function<CacheFactory<ID, R, RRC, CTX>, CacheFactory<ID, R, RRC, CTX>> {
    }

    class QueryFunctionException extends Exception {
        QueryFunctionException(Throwable t) {
            super(null, t, true, false);
        }
    }

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC>> CacheFactory<ID, R, RRC, CTX> cache() {

        final var delegateMap = new ConcurrentHashMap<ID, Sinks.One<RRC>>();

        Function<Iterable<ID>, Mono<Map<ID, RRC>>> getAll = ids -> resolve(readAll(ids, delegateMap, Empty::asMono));

        BiFunction<Iterable<ID>, FetchFunction<ID, RRC>, Mono<Map<ID, RRC>>> computeAll = (ids, fetchFunction) -> {

            final var cachedEntitiesMap = readAll(ids, delegateMap, Empty::asMono);

            final var missingIds = diff(ids, cachedEntitiesMap.keySet());
            if (isEmpty(missingIds)) {
                return resolve(cachedEntitiesMap);
            }

            final var sinkMap = also(createSinkMap(missingIds), delegateMap::putAll);

            return fetchFunction.apply(missingIds)
                    .doOnNext(resultMap -> sinkMap.forEach((id, sink) -> ofNullable(resultMap.get(id))
                            .ifPresentOrElse(sink::tryEmitValue, () -> {
                                delegateMap.remove(id);
                                sink.tryEmitEmpty();
                            })))
                    .doOnError(e -> sinkMap.forEach((id, sink) -> {
                        delegateMap.remove(id);
                        sink.tryEmitError(e);
                    }))
                    .flatMap(__ -> resolve(mergeMaps(cachedEntitiesMap, transformMapValues(sinkMap, Empty::asMono))));
        };

        Function<Map<ID, RRC>, Mono<?>> putAll = toMono(map -> also(createSinkMap(map.keySet()), delegateMap::putAll)
                .forEach((id, sink) -> sink.tryEmitValue(map.get(id))));

        Function<Map<ID, RRC>, Mono<?>> removeAll = toMono(map -> also(map.keySet(), ids -> delegateMap.keySet().removeAll(ids))
                .forEach(id -> ofNullable(delegateMap.get(id)).ifPresent(Empty::tryEmitEmpty)));

        return cache(getAll, computeAll, putAll, removeAll);
    }

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC>> CacheFactory<ID, R, RRC, CTX> cache(
            Function<Iterable<ID>, Mono<Map<ID, RRC>>> getAll,
            BiFunction<Iterable<ID>, FetchFunction<ID, RRC>, Mono<Map<ID, RRC>>> computeAll,
            Function<Map<ID, RRC>, Mono<?>> putAll,
            Function<Map<ID, RRC>, Mono<?>> removeAll) {

        return __ -> adapterCache(getAll, computeAll, putAll, removeAll);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, K, ID, R> RuleMapperSource<T, TC, K, ID, ID, R, R, OneToOneContext<T, TC, K, ID, R>> cached(
            Function<CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>, CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>>... delegateCacheFactories) {

        return cached(cache(), delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, K, ID, R> RuleMapperSource<T, TC, K, ID, ID, R, R, OneToOneContext<T, TC, K, ID, R>> cached(
            CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>> cache,
            Function<CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>, CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>>... delegateCacheFactories) {

        return cached(emptySource(), cache, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, K, ID, R> RuleMapperSource<T, TC, K, ID, ID, R, R, OneToOneContext<T, TC, K, ID, R>> cached(
            Function<TC, Publisher<R>> queryFunction,
            Function<CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>, CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>>... delegateCacheFactories) {

        return cached(from(queryFunction), delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, K, ID, R> RuleMapperSource<T, TC, K, ID, ID, R, R, OneToOneContext<T, TC, K, ID, R>> cached(
            RuleMapperSource<T, TC, K, ID, ID, R, R, OneToOneContext<T, TC, K, ID, R>> ruleMapperSource,
            Function<CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>, CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>>... delegateCacheFactories) {

        return cached(ruleMapperSource, cache(), delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, K, ID, R> RuleMapperSource<T, TC, K, ID, ID, R, R, OneToOneContext<T, TC, K, ID, R>> cached(
            Function<TC, Publisher<R>> queryFunction,
            CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>> cacheFactory,
            Function<CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>, CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>>... delegateCacheFactories) {

        return cached(from(queryFunction), cacheFactory, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, K, ID, R> RuleMapperSource<T, TC, K, ID, ID, R, R, OneToOneContext<T, TC, K, ID, R>> cached(
            RuleMapperSource<T, TC, K, ID, ID, R, R, OneToOneContext<T, TC, K, ID, R>> ruleMapperSource,
            CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>> cacheFactory,
            Function<CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>, CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>>>... delegateCacheFactories) {

        return cached(OneToOneCacheContext::new, ruleMapperSource, oneToOneCacheFactory(defer(cacheFactory)), delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, K, ID, EID, R, RC extends Collection<R>> RuleMapperSource<T, TC, K, ID, EID, R, RC, OneToManyContext<T, TC, K, ID, EID, R, RC>> cachedMany(
            Function<CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>>, CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>>>... delegateCacheFactories) {

        return cachedMany(cache(), null, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, K, ID, EID, R, RC extends Collection<R>> RuleMapperSource<T, TC, K, ID, EID, R, RC, OneToManyContext<T, TC, K, ID, EID, R, RC>> cachedMany(
            Comparator<R> sortComparator,
            Function<CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>>, CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>>>... delegateCacheFactories) {

        return cachedMany(cache(), sortComparator, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, K, ID, EID, R, RC extends Collection<R>> RuleMapperSource<T, TC, K, ID, EID, R, RC, OneToManyContext<T, TC, K, ID, EID, R, RC>> cachedMany(
            CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>> cacheFactory,
            Function<CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>>, CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>>>... delegateCacheFactories) {

        return cachedMany(emptySource(), cacheFactory, null, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, K, ID, EID, R, RC extends Collection<R>> RuleMapperSource<T, TC, K, ID, EID, R, RC, OneToManyContext<T, TC, K, ID, EID, R, RC>> cachedMany(
            CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>> cacheFactory,
            Comparator<R> sortComparator,
            Function<CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>>, CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>>>... delegateCacheFactories) {

        return cachedMany(emptySource(), cacheFactory, sortComparator, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, K, ID, EID, R, RC extends Collection<R>> RuleMapperSource<T, TC, K, ID, EID, R, RC, OneToManyContext<T, TC, K, ID, EID, R, RC>> cachedMany(
            Function<TC, Publisher<R>> queryFunction,
            Function<CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>>, CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>>>... delegateCacheFactories) {

        return cachedMany(queryFunction, (Comparator<R>) null,  delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, K, ID, EID, R, RC extends Collection<R>> RuleMapperSource<T, TC, K, ID, EID, R, RC, OneToManyContext<T, TC, K, ID, EID, R, RC>> cachedMany(
            Function<TC, Publisher<R>> queryFunction,
            Comparator<R> sortComparator,
            Function<CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>>, CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>>>... delegateCacheFactories) {

        return cachedMany(from(queryFunction), sortComparator, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, K, ID, EID, R, RC extends Collection<R>> RuleMapperSource<T, TC, K, ID, EID, R, RC, OneToManyContext<T, TC, K, ID, EID, R, RC>> cachedMany(
            RuleMapperSource<T, TC, K, ID, EID, R, RC, OneToManyContext<T, TC, K, ID, EID, R, RC>> ruleMapperSource,
            Function<CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>>, CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>>>... delegateCacheFactories) {

        return cachedMany(ruleMapperSource, (Comparator<R>) null, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, K, ID, EID, R, RC extends Collection<R>> RuleMapperSource<T, TC, K, ID, EID, R, RC, OneToManyContext<T, TC, K, ID, EID, R, RC>> cachedMany(
            RuleMapperSource<T, TC, K, ID, EID, R, RC, OneToManyContext<T, TC, K, ID, EID, R, RC>> ruleMapperSource,
            Comparator<R> sortComparator,
            Function<CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>>, CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>>>... delegateCacheFactories) {

        return cachedMany(ruleMapperSource, cache(), sortComparator, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, K, ID, EID, R, RC extends Collection<R>> RuleMapperSource<T, TC, K, ID, EID, R, RC, OneToManyContext<T, TC, K, ID, EID, R, RC>> cachedMany(
            Function<TC, Publisher<R>> queryFunction,
            CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>> cacheFactory,
            Function<CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>>, CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>>>... delegateCacheFactories) {

        return cachedMany(queryFunction, cacheFactory, null, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, K, ID, EID, R, RC extends Collection<R>> RuleMapperSource<T, TC, K, ID, EID, R, RC, OneToManyContext<T, TC, K, ID, EID, R, RC>> cachedMany(
            Function<TC, Publisher<R>> queryFunction,
            CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>> cacheFactory,
            Comparator<R> sortComparator,
            Function<CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>>, CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>>>... delegateCacheFactories) {

        return cachedMany(from(queryFunction), cacheFactory, sortComparator, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, K, ID, EID, R, RC extends Collection<R>> RuleMapperSource<T, TC, K, ID, EID, R, RC, OneToManyContext<T, TC, K, ID, EID, R, RC>> cachedMany(
            RuleMapperSource<T, TC, K, ID, EID, R, RC, OneToManyContext<T, TC, K, ID, EID, R, RC>> ruleMapperSource,
            CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>> cacheFactory,
            Function<CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>>, CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>>>... delegateCacheFactories) {

        return cachedMany(ruleMapperSource, cacheFactory, null, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, K, ID, EID, R, RC extends Collection<R>> RuleMapperSource<T, TC, K, ID, EID, R, RC, OneToManyContext<T, TC, K, ID, EID, R, RC>> cachedMany(
            RuleMapperSource<T, TC, K, ID, EID, R, RC, OneToManyContext<T, TC, K, ID, EID, R, RC>> ruleMapperSource,
            CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>> cacheFactory,
            Comparator<R> sortComparator,
            Function<CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>>, CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>>>... delegateCacheFactories) {

        return cached(OneToManyCacheContext::new, ruleMapperSource, oneToManyCacheFactory(sortBy(defer(cacheFactory), sortComparator)), delegateCacheFactories);
    }

    static <ID, RRC> Function<Map<ID, RRC>, Mono<?>> toMono(Consumer<Map<ID, RRC>> consumer) {
        return map -> just(also(map, consumer));
    }

    private static <ID, R> CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>> oneToOneCacheFactory(CacheFactory<ID, R, R, OneToOneCacheContext<ID, R>> cacheFactory) {
        return cacheContext -> oneToOneCache(cacheFactory.create(cacheContext));
    }

    private static <ID, EID, R, RC extends Collection<R>> CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>> oneToManyCacheFactory(CacheFactory<ID, R, RC, OneToManyCacheContext<ID, EID, R, RC>> cacheFactory) {
        return cacheContext -> oneToManyCache(cacheContext, cacheFactory.create(cacheContext));
    }

    @SafeVarargs
    private static <T, TC extends Collection<T>, K, ID, EID, R, RRC, CTX extends RuleMapperContext<T, TC, K, ID, EID, R, RRC>, CACHE_CTX extends CacheContext<ID, R, RRC>> RuleMapperSource<T, TC, K, ID, EID, R, RRC, CTX> cached(
            Function<CTX, CACHE_CTX> cacheContextProvider,
            RuleMapperSource<T, TC, K, ID, EID, R, RRC, CTX> ruleMapperSource,
            CacheFactory<ID, R, RRC, CACHE_CTX> cacheFactory,
            Function<CacheFactory<ID, R, RRC, CACHE_CTX>, CacheFactory<ID, R, RRC, CACHE_CTX>>... delegateCacheFactories) {

        final var isEmptySource = isEmptySource(ruleMapperSource);

        return ruleContext -> {
            final var queryFunction = nullToEmptySource(ruleMapperSource).apply(ruleContext);

            final var cache = delegate(cacheFactory, delegateCacheFactories)
                    .create(cacheContextProvider.apply(ruleContext));

            return entities -> then(ids(entities, ruleContext), ids -> isEmptySource ? cache.getAll(ids) : cache.computeAll(ids, buildFetchFunction(entities, nullToEmptySource(ruleMapperSource), ruleContext)))
                    .filter(CollectionUtils::isNotEmpty)
                    .flatMapMany(map -> fromStream(ruleContext.streamFlattener().apply(map.values().stream())))
                    .onErrorResume(not(QueryFunctionException.class::isInstance), __ -> queryFunction.apply(entities))
                    .onErrorMap(QueryFunctionException.class, Throwable::getCause);
        };
    }

    private static <T, TC extends Collection<T>, K, ID, EID, R, RRC> List<ID> ids(TC entities, RuleMapperContext<T, TC, K, ID, EID, R, RRC> ruleContext) {
        return transform(entities, ruleContext.outerIdResolver());
    }

    private static <T, TC extends Collection<T>, K, ID, EID, R, RRC, CTX extends RuleMapperContext<T, TC, K, ID, EID, R, RRC>> FetchFunction<ID, RRC> buildFetchFunction(
            TC entities,
            RuleMapperSource<T, TC, K, ID, EID, R, RRC, CTX> ruleMapperSource,
            CTX ruleContext) {

        return ids -> {
            if (isEmpty(ids)) {
                return just(Map.<ID, RRC>of());
            }

            final var idSet = new HashSet<>(asCollection(ids));

            final var entitiesToQuery = toStream(entities)
                    .filter(e -> idSet.contains(ruleContext.outerIdResolver().apply(e)))
                    .toList();

            return buildQueryFunction(ruleMapperSource, ruleContext).apply(entitiesToQuery)
                    .map(queryResultsMap -> buildCacheFragment(ids, queryResultsMap, ruleContext))
                    .onErrorMap(QueryFunctionException::new);
        };
    }

    @SafeVarargs
    private static <ID, R, RRC, CACHE_CTX extends CacheContext<ID, R, RRC>> CacheFactory<ID, R, RRC, CACHE_CTX> delegate(
            CacheFactory<ID, R, RRC, CACHE_CTX> cacheFactory,
            Function<CacheFactory<ID, R, RRC, CACHE_CTX>, CacheFactory<ID, R, RRC, CACHE_CTX>>... delegateCacheFactories) {

        return stream(delegateCacheFactories)
                .reduce(cacheFactory,
                        (previousCacheFactory, delegateCacheFactoryFunction) -> delegateCacheFactoryFunction.apply(previousCacheFactory),
                        (previousCacheFactory, decoratedCacheFactory) -> decoratedCacheFactory);
    }

    private static <T, TC extends Collection<T>, K, ID, EID, R, RRC> Map<ID, RRC> buildCacheFragment(
            Iterable<? extends ID> ids,
            Map<ID, RRC> queryResultsMap,
            RuleMapperContext<T, TC, K, ID, EID, R, RRC> ctx) {

        return newMap(queryResultsMap, map ->
                diff(ids, map.keySet())
                        .forEach(id -> ifNotNull(ctx.defaultResultProvider().apply(id), value -> map.put(id, value))));
    }
}
