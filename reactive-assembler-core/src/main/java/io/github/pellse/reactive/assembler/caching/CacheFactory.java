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

import static io.github.pellse.reactive.assembler.RuleMapperSource.call;
import static io.github.pellse.reactive.assembler.RuleMapperSource.emptyQuery;
import static io.github.pellse.reactive.assembler.caching.Cache.adapterCache;
import static io.github.pellse.reactive.assembler.caching.Cache.mergeStrategyAwareCache;
import static io.github.pellse.util.ObjectUtils.*;
import static io.github.pellse.util.collection.CollectionUtil.*;
import static java.lang.System.Logger.Level.WARNING;
import static java.lang.System.getLogger;
import static java.util.Arrays.stream;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.groupingBy;
import static reactor.core.publisher.Flux.from;
import static reactor.core.publisher.Flux.fromStream;
import static reactor.core.publisher.Mono.just;

@FunctionalInterface
public interface CacheFactory<ID, R, RRC> {

    static <ID, R, RRC> CacheFactory<ID, R, RRC> cache() {
        return cache(HashMap::new);
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> cache(Supplier<Map<ID, List<R>>> mapSupplier) {
        return cache(mapSupplier.get());
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> cache(Map<ID, List<R>> delegateMap) {

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

    static <ID, R, RRC> CacheFactory<ID, R, RRC> cache(
            BiFunction<Iterable<ID>, Boolean, Mono<Map<ID, List<R>>>> getAll,
            Function<Map<ID, List<R>>, Mono<?>> putAll,
            Function<Map<ID, List<R>>, Mono<?>> removeAll) {

        return (fetchFunction, __) -> adapterCache(getAll, putAll, removeAll);
    }

    @SafeVarargs
    static <ID, EID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, EID, IDC, R, RRC> cached(
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {
        return cached(cache(), delegateCacheFactories);
    }

    @SafeVarargs
    static <ID, EID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, EID, IDC, R, RRC> cached(
            CacheFactory<ID, R, RRC> cache,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {
        return cached(emptyQuery(), cache, delegateCacheFactories);
    }

    @SafeVarargs
    static <ID, EID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, EID, IDC, R, RRC> cached(
            Function<IDC, Publisher<R>> queryFunction,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {
        return cached(call(queryFunction), delegateCacheFactories);
    }

    @SafeVarargs
    static <ID, EID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, EID, IDC, R, RRC> cached(
            RuleMapperSource<ID, EID, IDC, R, RRC> ruleMapperSource,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {
        return cached(ruleMapperSource, cache(), delegateCacheFactories);
    }

    @SafeVarargs
    static <ID, EID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, EID, IDC, R, RRC> cached(
            Function<IDC, Publisher<R>> queryFunction,
            CacheFactory<ID, R, RRC> cacheFactory,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {
        return cached(call(queryFunction), cacheFactory, delegateCacheFactories);
    }

    @SafeVarargs
    static <ID, EID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, EID, IDC, R, RRC> cached(
            RuleMapperSource<ID, EID, IDC, R, RRC> ruleMapperSource,
            CacheFactory<ID, R, RRC> cacheFactory,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {

        var isCacheError = not(QueryFunctionException.class::isInstance);
        var logger = getLogger(CacheFactory.class.getName());
        Consumer<Throwable> logError = e -> logger.log(WARNING, "Recoverable error in cache, fall back to bypass cache and directly invoke fetchFunction:", e);

        return ruleContext -> {
            var queryFunction = ruleMapperSource.apply(ruleContext);
            Function<Iterable<? extends ID>, Mono<Map<ID, List<R>>>> fetchFunction =
                    entityIds -> then(translate(entityIds, ruleContext.idCollectionFactory()), ids ->
                            from(queryFunction.apply(ids))
                                    .collect(groupingBy(ruleContext.correlationIdExtractor()))
                                    .map(queryResultsMap -> buildCacheFragment(entityIds, queryResultsMap, ruleContext))
                                    .onErrorMap(QueryFunctionException::new));

            var cache = delegate(ruleContext, cacheFactory, delegateCacheFactories)
                    .create(fetchFunction, new CacheContext<>(ruleContext));

            return ids -> cache.getAll(ids, true)
                    .flatMapMany(map -> fromStream(map.values().stream().flatMap(Collection::stream)))
                    .doOnError(isCacheError, logError)
                    .onErrorResume(isCacheError, __ -> queryFunction.apply(ids))
                    .onErrorMap(QueryFunctionException.class, Throwable::getCause);
        };
    }

    static <ID, RRC> Function<Map<ID, RRC>, Mono<?>> toMono(Consumer<Map<ID, RRC>> consumer) {
        return map -> just(also(map, consumer));
    }

    @SafeVarargs
    private static <ID, EID, IDC extends Collection<ID>, R, RRC> CacheFactory<ID, R, RRC> delegate(
            RuleMapperContext<ID, EID, IDC, R, RRC> ruleContext,
            CacheFactory<ID, R, RRC> cacheFactory,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {

        return ConcurrentCacheFactory.<ID, R, RRC>concurrent().apply(
                stream(delegateCacheFactories)
                        .reduce((fetchFunction, context) -> mergeStrategyAwareCache(ruleContext.idExtractor(), cacheFactory.create(fetchFunction, context)),
                                (previousCacheFactory, delegateCacheFactoryFunction) -> delegateCacheFactoryFunction.apply(previousCacheFactory),
                                (previousCacheFactory, decoratedCacheFactory) -> decoratedCacheFactory));
    }

    private static <ID, EID, IDC extends Collection<ID>, R, RRC> Map<ID, List<R>> buildCacheFragment(
            Iterable<? extends ID> entityIds,
            Map<ID, List<R>> queryResultsMap,
            RuleMapperContext<ID, EID, IDC, R, RRC> ctx) {

        return newMap(queryResultsMap, map ->
                intersect(entityIds, map.keySet()).forEach(id ->
                        ifNotNull(ctx.defaultResultProvider().apply(id), value -> map.put(id, ctx.toListConverter().apply(value)))));
    }

    Cache<ID, R> create(
            Function<Iterable<? extends ID>, Mono<Map<ID, List<R>>>> fetchFunction,
            CacheContext<ID, R, RRC> context);

    interface CacheTransformer<ID, R, RRC> extends Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> {
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

        public CacheContext(RuleMapperContext<ID, ?, ?, R, RRC> ctx) {
            this(ctx.correlationIdExtractor(), ctx.fromListConverter(), ctx.toListConverter());
        }
    }
}
