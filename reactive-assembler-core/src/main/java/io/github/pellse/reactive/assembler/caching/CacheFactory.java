/*
 * Copyright 2023 Sebastien Pelletier
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

package io.github.pellse.reactive.assembler.caching;

import io.github.pellse.reactive.assembler.RuleMapperContext;
import io.github.pellse.reactive.assembler.RuleMapperSource;
import io.github.pellse.reactive.assembler.caching.CacheFactory.FetchFunction.EmptyFetchFunction;
import io.github.pellse.reactive.assembler.caching.CacheFactory.FetchFunction.NonEmptyFetchFunction;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.github.pellse.reactive.assembler.RuleMapperSource.*;
import static io.github.pellse.reactive.assembler.caching.Cache.adapterCache;
import static io.github.pellse.reactive.assembler.caching.Cache.mergeStrategyAwareCache;
import static io.github.pellse.util.ObjectUtils.*;
import static io.github.pellse.util.collection.CollectionUtil.*;
import static java.util.Arrays.stream;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.groupingBy;
import static reactor.core.publisher.Flux.from;
import static reactor.core.publisher.Flux.fromStream;
import static reactor.core.publisher.Mono.just;

@FunctionalInterface
public interface CacheFactory<T, ID, R, RRC> {

    static <T, ID, R, RRC> CacheFactory<T, ID, R, RRC> cache() {
        return cache(HashMap::new);
    }

    static <T, ID, R, RRC> CacheFactory<T, ID, R, RRC> cache(Supplier<Map<ID, List<R>>> mapSupplier) {
        return cache(mapSupplier.get());
    }

    static <T, ID, R, RRC> CacheFactory<T, ID, R, RRC> cache(Map<ID, List<R>> delegateMap) {

        return (fetchFunction, __) -> adapterCache(
                (ids, computeIfAbsent) -> just(readAll(ids, delegateMap))
                        .flatMap(cachedEntitiesMap -> then(intersect(ids, cachedEntitiesMap.keySet()), entityIds ->
                                !computeIfAbsent || entityIds.isEmpty() ? just(cachedEntitiesMap) :
                                        fetchFunction.apply(entityIds)
                                                .doOnNext(delegateMap::putAll)
                                                .map(map -> mergeMaps(map, cachedEntitiesMap)))),
                toMono(delegateMap::putAll),
                toMono(map -> delegateMap.keySet().removeAll(map.keySet())));
    }

    static <T, ID, R, RRC> CacheFactory<T, ID, R, RRC> cache(
            BiFunction<Iterable<ID>, Boolean, Mono<Map<ID, List<R>>>> getAll,
            Function<Map<ID, List<R>>, Mono<?>> putAll,
            Function<Map<ID, List<R>>, Mono<?>> removeAll) {

        return (fetchFunction, __) -> adapterCache(getAll, putAll, removeAll);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, ID, EID, R, RRC> RuleMapperSource<T, TC, ID, EID, R, RRC> cached(
            Function<CacheFactory<T, ID, R, RRC>, CacheFactory<T, ID, R, RRC>>... delegateCacheFactories) {
        return cached(cache(), delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, ID, EID, R, RRC> RuleMapperSource<T, TC, ID, EID, R, RRC> cached(
            CacheFactory<T, ID, R, RRC> cache,
            Function<CacheFactory<T, ID, R, RRC>, CacheFactory<T, ID, R, RRC>>... delegateCacheFactories) {
        return cached(emptySource(), cache, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, ID, EID, R, RRC> RuleMapperSource<T, TC, ID, EID, R, RRC> cached(
            Function<TC, Publisher<R>> queryFunction,
            Function<CacheFactory<T, ID, R, RRC>, CacheFactory<T, ID, R, RRC>>... delegateCacheFactories) {
        return cached(call(queryFunction), delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, ID, EID, R, RRC> RuleMapperSource<T, TC, ID, EID, R, RRC> cached(
            RuleMapperSource<T, TC, ID, EID, R, RRC> ruleMapperSource,
            Function<CacheFactory<T, ID, R, RRC>, CacheFactory<T, ID, R, RRC>>... delegateCacheFactories) {
        return cached(ruleMapperSource, cache(), delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, ID, EID, R, RRC> RuleMapperSource<T, TC, ID, EID, R, RRC> cached(
            Function<TC, Publisher<R>> queryFunction,
            CacheFactory<T, ID, R, RRC> cacheFactory,
            Function<CacheFactory<T, ID, R, RRC>, CacheFactory<T, ID, R, RRC>>... delegateCacheFactories) {
        return cached(call(queryFunction), cacheFactory, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, ID, EID, R, RRC> RuleMapperSource<T, TC, ID, EID, R, RRC> cached(
            RuleMapperSource<T, TC, ID, EID, R, RRC> ruleMapperSource,
            CacheFactory<T, ID, R, RRC> cacheFactory,
            Function<CacheFactory<T, ID, R, RRC>, CacheFactory<T, ID, R, RRC>>... delegateCacheFactories) {

        return ruleContext -> {
            final var queryFunction =  nullToEmptySource(ruleMapperSource).apply(ruleContext);

            final NonEmptyFetchFunction<T, ID, R> fetchFunction =
                    entityIds -> then(translate(entityIds, ruleContext.topLevelCollectionFactory()), ids ->
                            from(queryFunction.apply(ids))
                                    .collect(groupingBy(ruleContext.correlationIdExtractor()))
                                    .map(queryResultsMap -> buildCacheFragment(entityIds, queryResultsMap, ruleContext))
                                    .onErrorMap(QueryFunctionException::new));

            final EmptyFetchFunction<T, ID, R> emptyFetchFunction = ids -> Mono.empty();

            final var cache = delegate(ruleContext, cacheFactory, delegateCacheFactories)
                    .create(isEmptySource(ruleMapperSource) ? emptyFetchFunction : fetchFunction, new CacheContext<>(ruleContext));

            return entities -> cache.getAll(transform(entities, ruleContext.topLevelIdExtractor()), true)
                    .flatMapMany(map -> fromStream(map.values().stream().flatMap(Collection::stream)))
                    .onErrorResume(not(QueryFunctionException.class::isInstance), __ -> queryFunction.apply(entities))
                    .onErrorMap(QueryFunctionException.class, Throwable::getCause);
        };
    }

    static <ID, RRC> Function<Map<ID, RRC>, Mono<?>> toMono(Consumer<Map<ID, RRC>> consumer) {
        return map -> just(also(map, consumer));
    }

    @SafeVarargs
    private static <T, TC extends Collection<T>, ID, EID, R, RRC> CacheFactory<T, ID, R, RRC> delegate(
            RuleMapperContext<T, TC, ID, EID, R, RRC> ruleContext,
            CacheFactory<T, ID, R, RRC> cacheFactory,
            Function<CacheFactory<T, ID, R, RRC>, CacheFactory<T, ID, R, RRC>>... delegateCacheFactories) {

        return ConcurrentCacheFactory.<T, ID, R, RRC>concurrent().apply(
                stream(delegateCacheFactories)
                        .reduce((fetchFunction, context) -> mergeStrategyAwareCache(ruleContext.idExtractor(), cacheFactory.create(fetchFunction, context)),
                                (previousCacheFactory, delegateCacheFactoryFunction) -> delegateCacheFactoryFunction.apply(previousCacheFactory),
                                (previousCacheFactory, decoratedCacheFactory) -> decoratedCacheFactory)
        );
    }

    private static <T, TC extends Collection<T>, ID, EID, R, RRC> Map<ID, List<R>> buildCacheFragment(
            Iterable<? extends ID> entityIds,
            Map<ID, List<R>> queryResultsMap,
            RuleMapperContext<T, TC, ID, EID, R, RRC> ctx) {

        return newMap(queryResultsMap, map ->
                intersect(entityIds, map.keySet()).forEach(id ->
                        ifNotNull(ctx.defaultResultProvider().apply(id), value -> map.put(id, ctx.toListConverter().apply(value)))));
    }

    Cache<T, ID, R> create(FetchFunction<T, ID, R> fetchFunction,  CacheContext<ID, R, RRC> context);

    interface CacheTransformer<T, ID, R, RRC> extends Function<CacheFactory<T, ID, R, RRC>, CacheFactory<T, ID, R, RRC>> {
    }

    sealed interface FetchFunction<T, ID, R> extends Function<Iterable<? extends T>, Mono<Map<ID, List<R>>>> {
        non-sealed interface EmptyFetchFunction<T, ID, R> extends FetchFunction<T, ID, R> {}
        non-sealed interface NonEmptyFetchFunction<T, ID, R> extends FetchFunction<T, ID, R> {}
    }

    class QueryFunctionException extends Exception {
        QueryFunctionException(Throwable t) {
            super(null, t, true, false);
        }
    }

    record CacheContext<ID, R, RRC>(
            Function<R, ID> correlationIdExtractor,
            Function<List<R>, RRC> fromListConverter,
            Function<RRC, List<R>> toListConverter) {

        public CacheContext(RuleMapperContext<?, ?, ID, ?, R, RRC> ctx) {
            this(ctx.correlationIdExtractor(), ctx.fromListConverter(), ctx.toListConverter());
        }
    }
}
