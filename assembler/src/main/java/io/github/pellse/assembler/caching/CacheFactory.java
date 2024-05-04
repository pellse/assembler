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
import static io.github.pellse.assembler.caching.Cache.adapterCache;
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
public interface CacheFactory<ID, R, RRC> {

    Cache<ID, RRC> create(CacheContext<ID, R, RRC> context);

    @FunctionalInterface
    interface CacheTransformer<ID, R, RRC> extends Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> {
    }

    class QueryFunctionException extends Exception {
        QueryFunctionException(Throwable t) {
            super(null, t, true, false);
        }
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> cache() {

        final var delegateMap = new ConcurrentHashMap<ID, Sinks.One<RRC>>();

        Function<Iterable<ID>, Mono<Map<ID, RRC>>> getAll = ids -> resolve(readAll(ids, delegateMap, Empty::asMono));

        BiFunction<Iterable<ID>, FetchFunction<ID, RRC>, Mono<Map<ID, RRC>>> computeAll = (ids, fetchFunction) -> {

            final var cachedEntitiesMap = readAll(ids, delegateMap, Empty::asMono);

            final var missingIds = intersect(ids, cachedEntitiesMap.keySet());
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
                    .flatMap(__ -> resolve(mergeMaps(transformMap(sinkMap, Empty::asMono), cachedEntitiesMap)));
        };

        Function<Map<ID, RRC>, Mono<?>> putAll = toMono(map -> also(createSinkMap(map.keySet()), delegateMap::putAll)
                .forEach((id, sink) -> sink.tryEmitValue(map.get(id))));

        Function<Map<ID, RRC>, Mono<?>> removeAll = toMono(map -> also(map.keySet(), ids -> delegateMap.keySet().removeAll(ids))
                .forEach(id -> ofNullable(delegateMap.get(id)).ifPresent(Empty::tryEmitEmpty)));

        return cache(getAll, computeAll, putAll, removeAll);
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> cache(
            Function<Iterable<ID>, Mono<Map<ID, RRC>>> getAll,
            BiFunction<Iterable<ID>, FetchFunction<ID, RRC>, Mono<Map<ID, RRC>>> computeAll,
            Function<Map<ID, RRC>, Mono<?>> putAll,
            Function<Map<ID, RRC>, Mono<?>> removeAll) {

        return __ -> adapterCache(getAll, computeAll, putAll, removeAll);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, ID, R> RuleMapperSource<T, TC, ID, ID, R, R, OneToOneContext<T, TC, ID, R>> cached(
            Function<CacheFactory<ID, R, R>, CacheFactory<ID, R, R>>... delegateCacheFactories) {

        return cached(cache(), delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, ID, R> RuleMapperSource<T, TC, ID, ID, R, R, OneToOneContext<T, TC, ID, R>> cached(
            CacheFactory<ID, R, R> cache,
            Function<CacheFactory<ID, R, R>, CacheFactory<ID, R, R>>... delegateCacheFactories) {

        return cached(emptySource(), cache, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, ID, R> RuleMapperSource<T, TC, ID, ID, R, R, OneToOneContext<T, TC, ID, R>> cached(
            Function<TC, Publisher<R>> queryFunction,
            Function<CacheFactory<ID, R, R>, CacheFactory<ID, R, R>>... delegateCacheFactories) {

        return cached(toRuleMapperSource(queryFunction), delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, ID, R> RuleMapperSource<T, TC, ID, ID, R, R, OneToOneContext<T, TC, ID, R>> cached(
            RuleMapperSource<T, TC, ID, ID, R, R, OneToOneContext<T, TC, ID, R>> ruleMapperSource,
            Function<CacheFactory<ID, R, R>, CacheFactory<ID, R, R>>... delegateCacheFactories) {

        return cached(ruleMapperSource, cache(), delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, ID, R> RuleMapperSource<T, TC, ID, ID, R, R, OneToOneContext<T, TC, ID, R>> cached(
            Function<TC, Publisher<R>> queryFunction,
            CacheFactory<ID, R, R> cacheFactory,
            Function<CacheFactory<ID, R, R>, CacheFactory<ID, R, R>>... delegateCacheFactories) {

        return cached(toRuleMapperSource(queryFunction), cacheFactory, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, ID, R> RuleMapperSource<T, TC, ID, ID, R, R, OneToOneContext<T, TC, ID, R>> cached(
            RuleMapperSource<T, TC, ID, ID, R, R, OneToOneContext<T, TC, ID, R>> ruleMapperSource,
            CacheFactory<ID, R, R> cacheFactory,
            Function<CacheFactory<ID, R, R>, CacheFactory<ID, R, R>>... delegateCacheFactories) {

        return cached(OneToOneCacheContext::new, ruleMapperSource, cacheFactory, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, ID, EID, R, RC extends Collection<R>> RuleMapperSource<T, TC, ID, EID, R, RC, OneToManyContext<T, TC, ID, EID, R, RC>> cachedMany(
            Function<CacheFactory<ID, R, RC>, CacheFactory<ID, R, RC>>... delegateCacheFactories) {

        return cachedMany(cache(), delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, ID, EID, R, RC extends Collection<R>> RuleMapperSource<T, TC, ID, EID, R, RC, OneToManyContext<T, TC, ID, EID, R, RC>> cachedMany(
            CacheFactory<ID, R, RC> cache,
            Function<CacheFactory<ID, R, RC>, CacheFactory<ID, R, RC>>... delegateCacheFactories) {

        return cachedMany(emptySource(), cache, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, ID, EID, R, RC extends Collection<R>> RuleMapperSource<T, TC, ID, EID, R, RC, OneToManyContext<T, TC, ID, EID, R, RC>> cachedMany(
            Function<TC, Publisher<R>> queryFunction,
            Function<CacheFactory<ID, R, RC>, CacheFactory<ID, R, RC>>... delegateCacheFactories) {

        return cachedMany(toRuleMapperSource(queryFunction), delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, ID, EID, R, RC extends Collection<R>> RuleMapperSource<T, TC, ID, EID, R, RC, OneToManyContext<T, TC, ID, EID, R, RC>> cachedMany(
            RuleMapperSource<T, TC, ID, EID, R, RC, OneToManyContext<T, TC, ID, EID, R, RC>> ruleMapperSource,
            Function<CacheFactory<ID, R, RC>, CacheFactory<ID, R, RC>>... delegateCacheFactories) {

        return cachedMany(ruleMapperSource, cache(), delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, ID, EID, R, RC extends Collection<R>> RuleMapperSource<T, TC, ID, EID, R, RC, OneToManyContext<T, TC, ID, EID, R, RC>> cachedMany(
            Function<TC, Publisher<R>> queryFunction,
            CacheFactory<ID, R, RC> cacheFactory,
            Function<CacheFactory<ID, R, RC>, CacheFactory<ID, R, RC>>... delegateCacheFactories) {

        return cachedMany(toRuleMapperSource(queryFunction), cacheFactory, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, ID, EID, R, RC extends Collection<R>> RuleMapperSource<T, TC, ID, EID, R, RC, OneToManyContext<T, TC, ID, EID, R, RC>> cachedMany(
            RuleMapperSource<T, TC, ID, EID, R, RC, OneToManyContext<T, TC, ID, EID, R, RC>> ruleMapperSource,
            CacheFactory<ID, R, RC> cacheFactory,
            Function<CacheFactory<ID, R, RC>, CacheFactory<ID, R, RC>>... delegateCacheFactories) {

        return cached(OneToManyCacheContext::new, ruleMapperSource, cacheFactory, delegateCacheFactories);
    }

    static <ID, RRC> Function<Map<ID, RRC>, Mono<?>> toMono(Consumer<Map<ID, RRC>> consumer) {
        return map -> just(also(map, consumer));
    }

    @SafeVarargs
    private static <T, TC extends Collection<T>, ID, EID, R, RRC, CTX extends RuleMapperContext<T, TC, ID, EID, R, RRC>> RuleMapperSource<T, TC, ID, EID, R, RRC, CTX> cached(
            BiFunction<Boolean, CTX, CacheContext<ID, R, RRC>> cacheContextProvider,
            RuleMapperSource<T, TC, ID, EID, R, RRC, CTX> ruleMapperSource,
            CacheFactory<ID, R, RRC> cacheFactory,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {

        final var isEmptySource = isEmptySource(ruleMapperSource);

        return ruleContext -> {
            final var queryFunction = nullToEmptySource(ruleMapperSource).apply(ruleContext);
            final var cacheContext = cacheContextProvider.apply(isEmptySource, ruleContext);

            final var cache = delegate(cacheContext::mergeStrategyAwareCache, cacheFactory, delegateCacheFactories)
                    .create(cacheContext);

            return entities -> then(ids(entities, ruleContext), ids -> isEmptySource ? cache.getAll(ids) : cache.computeAll(ids, buildFetchFunction(entities, nullToEmptySource(ruleMapperSource), ruleContext)))
                    .filter(CollectionUtils::isNotEmpty)
                    .flatMapMany(map -> fromStream(ruleContext.streamFlattener().apply(map.values().stream())))
                    .onErrorResume(not(QueryFunctionException.class::isInstance), __ -> queryFunction.apply(entities))
                    .onErrorMap(QueryFunctionException.class, Throwable::getCause);
        };
    }

    private static <T, TC extends Collection<T>, ID, EID, R, RRC> List<ID> ids(TC entities, RuleMapperContext<T, TC, ID, EID, R, RRC> ruleContext) {
        return transform(entities, ruleContext.topLevelIdResolver());
    }

    private static <T, TC extends Collection<T>, ID, EID, R, RRC, CTX extends RuleMapperContext<T, TC, ID, EID, R, RRC>> FetchFunction<ID, RRC> buildFetchFunction(
            TC entities,
            RuleMapperSource<T, TC, ID, EID, R, RRC, CTX> ruleMapperSource,
            CTX ruleContext) {

        return ids -> {
            final Set<ID> idSet = new HashSet<>(asCollection(ids));

            final var entitiesToQuery = toStream(entities)
                    .filter(e -> idSet.contains(ruleContext.topLevelIdResolver().apply(e)))
                    .toList();

            return buildQueryFunction(ruleMapperSource, ruleContext).apply(entitiesToQuery)
                    .map(queryResultsMap -> buildCacheFragment(ids, queryResultsMap, ruleContext))
                    .onErrorMap(QueryFunctionException::new);
        };
    }

    @SafeVarargs
    private static <ID, R, RRC> CacheFactory<ID, R, RRC> delegate(
            Function<Cache<ID, RRC>, Cache<ID, RRC>> mergeStrategyAwareCacheProvider,
            CacheFactory<ID, R, RRC> cacheFactory,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {

        return stream(delegateCacheFactories)
                .reduce(context -> mergeStrategyAwareCacheProvider.apply(cacheFactory.create(context)),
                        (previousCacheFactory, delegateCacheFactoryFunction) -> delegateCacheFactoryFunction.apply(previousCacheFactory),
                        (previousCacheFactory, decoratedCacheFactory) -> decoratedCacheFactory);
    }

    private static <T, TC extends Collection<T>, ID, EID, R, RRC> Map<ID, RRC> buildCacheFragment(
            Iterable<? extends ID> ids,
            Map<ID, RRC> queryResultsMap,
            RuleMapperContext<T, TC, ID, EID, R, RRC> ctx) {

        return newMap(queryResultsMap, map ->
                intersect(ids, map.keySet()).forEach(id ->
                        ifNotNull(ctx.defaultResultProvider().apply(id), value -> map.put(id, value))));
    }
}
