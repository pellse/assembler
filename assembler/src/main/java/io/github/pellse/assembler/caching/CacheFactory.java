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
import io.github.pellse.assembler.RuleMapperSource;
import io.github.pellse.assembler.caching.Cache.FetchFunction;
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

import static io.github.pellse.assembler.RuleMapperSource.*;
import static io.github.pellse.assembler.caching.Cache.adapterCache;
import static io.github.pellse.assembler.caching.Cache.mergeStrategyAwareCache;
import static io.github.pellse.util.ObjectUtils.*;
import static io.github.pellse.util.collection.CollectionUtils.*;
import static io.github.pellse.util.reactive.ReactiveUtils.createSinkMap;
import static io.github.pellse.util.reactive.ReactiveUtils.resolve;
import static java.util.Arrays.stream;
import static java.util.Optional.ofNullable;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.groupingBy;
import static reactor.core.publisher.Flux.from;
import static reactor.core.publisher.Flux.fromStream;
import static reactor.core.publisher.Mono.just;

@FunctionalInterface
public interface CacheFactory<ID, R, RRC> {

    static <ID, R, RRC> CacheFactory<ID, R, RRC> cache() {

        final var delegateMap = new ConcurrentHashMap<ID, Sinks.One<List<R>>>();

        Function<Iterable<ID>, Mono<Map<ID, List<R>>>> getAll = ids -> resolve(readAll(ids, delegateMap, Empty::asMono));

        BiFunction<Iterable<ID>, FetchFunction<ID, R>, Mono<Map<ID, List<R>>>> computeAll = (ids, fetchFunction) -> {

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

        Function<Map<ID, List<R>>, Mono<?>> putAll = toMono(map -> also(createSinkMap(map.keySet()), delegateMap::putAll)
                .forEach((id, sink) -> sink.tryEmitValue(map.get(id))));

        Function<Map<ID, List<R>>, Mono<?>> removeAll = toMono(map -> also(map.keySet(), ids -> delegateMap.keySet().removeAll(ids))
                .forEach(id -> ofNullable(delegateMap.get(id)).ifPresent(Empty::tryEmitEmpty)));

        return cache(getAll, computeAll, putAll, removeAll);
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> cache(
            Function<Iterable<ID>, Mono<Map<ID, List<R>>>> getAll,
            BiFunction<Iterable<ID>, FetchFunction<ID, R>, Mono<Map<ID, List<R>>>> computeAll,
            Function<Map<ID, List<R>>, Mono<?>> putAll,
            Function<Map<ID, List<R>>, Mono<?>> removeAll) {

        return __ -> adapterCache(getAll, computeAll, putAll, removeAll);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, ID, EID, R, RRC> RuleMapperSource<T, TC, ID, EID, R, RRC> cached(
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {

        return cached(cache(), delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, ID, EID, R, RRC> RuleMapperSource<T, TC, ID, EID, R, RRC> cached(
            CacheFactory<ID, R, RRC> cache,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {

        return cached(emptySource(), cache, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, ID, EID, R, RRC> RuleMapperSource<T, TC, ID, EID, R, RRC> cached(
            Function<TC, Publisher<R>> queryFunction,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {

        return cached(toQueryFunction(queryFunction), delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, ID, EID, R, RRC> RuleMapperSource<T, TC, ID, EID, R, RRC> cached(
            RuleMapperSource<T, TC, ID, EID, R, RRC> ruleMapperSource,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {

        return cached(ruleMapperSource, cache(), delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, ID, EID, R, RRC> RuleMapperSource<T, TC, ID, EID, R, RRC> cached(
            Function<TC, Publisher<R>> queryFunction,
            CacheFactory<ID, R, RRC> cacheFactory,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {

        return cached(toQueryFunction(queryFunction), cacheFactory, delegateCacheFactories);
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, ID, EID, R, RRC> RuleMapperSource<T, TC, ID, EID, R, RRC> cached(
            RuleMapperSource<T, TC, ID, EID, R, RRC> ruleMapperSource,
            CacheFactory<ID, R, RRC> cacheFactory,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {

        final var isEmptySource = isEmptySource(ruleMapperSource);

        return ruleContext -> {
            final var queryFunction = nullToEmptySource(ruleMapperSource).apply(ruleContext);

            final var cache = delegate(ruleContext, cacheFactory, delegateCacheFactories)
                    .create(new CacheContext<>(isEmptySource, ruleContext));

            return entities -> then(ids(entities, ruleContext), ids -> isEmptySource ? cache.getAll(ids) : cache.computeAll(ids, buildFetchFunction(entities, ruleContext, queryFunction)))
                    .filter(CollectionUtils::isNotEmpty)
                    .flatMapMany(map -> fromStream(map.values().stream().flatMap(Collection::stream)))
                    .onErrorResume(not(QueryFunctionException.class::isInstance), __ -> queryFunction.apply(entities))
                    .onErrorMap(QueryFunctionException.class, Throwable::getCause);
        };
    }

    static <ID, RRC> Function<Map<ID, RRC>, Mono<?>> toMono(Consumer<Map<ID, RRC>> consumer) {
        return map -> just(also(map, consumer));
    }

    private static <T, TC extends Collection<T>, ID, EID, R, RRC> List<ID> ids(TC entities, RuleMapperContext<T, TC, ID, EID, R, RRC> ruleContext) {
        return transform(entities, ruleContext.topLevelIdResolver());
    }

    private static <T, TC extends Collection<T>, ID, EID, R, RRC> FetchFunction<ID, R> buildFetchFunction(
            TC entities,
            RuleMapperContext<T, TC, ID, EID, R, RRC> ruleContext,
            Function<TC, Publisher<R>> queryFunction) {

        return ids -> {

            final Set<ID> idSet = new HashSet<>(asCollection(ids));

            final var entitiesToQuery = toStream(entities)
                    .filter(e -> idSet.contains(ruleContext.topLevelIdResolver().apply(e)))
                    .toList();

            return from(queryFunction.apply(translate(entitiesToQuery, ruleContext.topLevelCollectionFactory())))
                    .collect(groupingBy(ruleContext.correlationIdResolver()))
                    .map(queryResultsMap -> buildCacheFragment(ids, queryResultsMap, ruleContext))
                    .onErrorMap(QueryFunctionException::new);
        };
    }

    @SafeVarargs
    private static <T, TC extends Collection<T>, ID, EID, R, RRC> CacheFactory<ID, R, RRC> delegate(
            RuleMapperContext<T, TC, ID, EID, R, RRC> ruleContext,
            CacheFactory<ID, R, RRC> cacheFactory,
            Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>... delegateCacheFactories) {

        return stream(delegateCacheFactories)
                .reduce(context -> mergeStrategyAwareCache(ruleContext, cacheFactory.create(context)),
                        (previousCacheFactory, delegateCacheFactoryFunction) -> delegateCacheFactoryFunction.apply(previousCacheFactory),
                        (previousCacheFactory, decoratedCacheFactory) -> decoratedCacheFactory);

//        return ConcurrentCacheFactory.<ID, R, RRC>concurrent().apply(
//                stream(delegateCacheFactories)
//                        .reduce(context -> mergeStrategyAwareCache(ruleContext.idResolver(), cacheFactory.create(context)),
//                                (previousCacheFactory, delegateCacheFactoryFunction) -> delegateCacheFactoryFunction.apply(previousCacheFactory),
//                                (previousCacheFactory, decoratedCacheFactory) -> decoratedCacheFactory)
//        );
    }

    private static <T, TC extends Collection<T>, ID, EID, R, RRC> Map<ID, List<R>> buildCacheFragment(
            Iterable<? extends ID> ids,
            Map<ID, List<R>> queryResultsMap,
            RuleMapperContext<T, TC, ID, EID, R, RRC> ctx) {

        return newMap(queryResultsMap, map ->
                intersect(ids, map.keySet()).forEach(id ->
                        ifNotNull(ctx.defaultResultProvider().apply(id), value -> map.put(id, ctx.toListConverter().apply(value)))));
    }

    Cache<ID, R> create(CacheContext<ID, R, RRC> context);

    interface CacheTransformer<ID, R, RRC> extends Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> {
    }

    class QueryFunctionException extends Exception {

        QueryFunctionException(Throwable t) {
            super(null, t, true, false);
        }
    }

    record CacheContext<ID, R, RRC>(
            boolean isEmptySource,
            Function<R, ID> correlationIdResolver,
            Function<List<R>, RRC> fromListConverter,
            Function<RRC, List<R>> toListConverter) {

        public CacheContext(boolean isEmptySource, RuleMapperContext<?, ?, ID, ?, R, RRC> ctx) {
            this(isEmptySource, ctx.correlationIdResolver(), ctx.fromListConverter(), ctx.toListConverter());
        }
    }
}
