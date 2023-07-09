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

package io.github.pellse.cohereflux.caching;

import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.github.pellse.util.ObjectUtils.then;
import static io.github.pellse.util.collection.CollectionUtils.*;
import static java.util.Map.of;
import static java.util.Optional.ofNullable;
import static reactor.core.publisher.Mono.defer;
import static reactor.core.publisher.Mono.just;

@FunctionalInterface
interface CacheUpdater<ID, R> {

    Mono<?> updateCache(Cache<ID, R> cache, Map<ID, List<R>> cacheQueryResults, Map<ID, List<R>> incomingChanges);
}

public interface Cache<ID, R> {

    static <ID, R> Cache<ID, R> adapterCache(
            BiFunction<Iterable<ID>, FetchFunction<ID, R>, Mono<Map<ID, List<R>>>> getAll,
            Function<Map<ID, List<R>>, Mono<?>> putAll,
            Function<Map<ID, List<R>>, Mono<?>> removeAll) {

        return adapterCache(getAll, putAll, removeAll, null);
    }

    static <ID, R> Cache<ID, R> adapterCache(
            BiFunction<Iterable<ID>, FetchFunction<ID, R>, Mono<Map<ID, List<R>>>> getAll,
            Function<Map<ID, List<R>>, Mono<?>> putAll,
            Function<Map<ID, List<R>>, Mono<?>> removeAll,
            BiFunction<Map<ID, List<R>>, Map<ID, List<R>>, Mono<?>> updateAll) {

        return new Cache<>() {

            @Override
            public Mono<Map<ID, List<R>>> getAll(Iterable<ID> ids, FetchFunction<ID, R> fetchFunction) {
                return getAll.apply(ids, fetchFunction);
            }

            @Override
            public Mono<?> putAll(Map<ID, List<R>> map) {
                return putAll.apply(map);
            }

            @Override
            public Mono<?> removeAll(Map<ID, List<R>> map) {
                return removeAll.apply(map);
            }

            @Override
            public Mono<?> updateAll(Map<ID, List<R>> mapToAdd, Map<ID, List<R>> mapToRemove) {
                return ofNullable(updateAll)
                        .orElse(Cache.super::updateAll)
                        .apply(mapToAdd, mapToRemove);
            }
        };
    }

    static <ID, EID, R> Cache<ID, R> mergeStrategyAwareCache(
            Function<R, EID> idResolver,
            Cache<ID, R> delegateCache) {

        final var optimizedCache = adapterCache(
                emptyOr(delegateCache::getAll),
                emptyOr(delegateCache::putAll),
                emptyOr(delegateCache::removeAll)
        );

        return adapterCache(
                optimizedCache::getAll,
                applyMergeStrategy(
                        optimizedCache,
                        (cacheQueryResults, incomingChanges) -> mergeMaps(incomingChanges, cacheQueryResults, idResolver),
                        Cache::putAll),
                applyMergeStrategy(
                        optimizedCache,
                        (cache, cacheQueryResults, incomingChanges) ->
                                then(subtractFromMap(incomingChanges, cacheQueryResults, idResolver),
                                        updatedMap -> cache.updateAll(updatedMap, diff(cacheQueryResults, updatedMap))))
        );
    }

    private static <ID, R> Function<Map<ID, List<R>>, Mono<?>> applyMergeStrategy(
            Cache<ID, R> delegateCache,
            MergeStrategy<ID, R> mergeStrategy,
            BiFunction<Cache<ID, R>, Map<ID, List<R>>, Mono<?>> cacheUpdater) {

        return applyMergeStrategy(
                delegateCache,
                (cache, cacheQueryResults, incomingChanges) ->
                        cacheUpdater.apply(cache, mergeStrategy.merge(cacheQueryResults, incomingChanges)));
    }

    private static <ID, R> Function<Map<ID, List<R>>, Mono<?>> applyMergeStrategy(
            Cache<ID, R> delegateCache,
            CacheUpdater<ID, R> cacheUpdater) {

        return incomingChanges -> isEmpty(incomingChanges) ? just(of()) : defer(() ->
                delegateCache.getAll(incomingChanges.keySet(), ids -> just(of()))
                        .flatMap(cacheQueryResults -> cacheUpdater.updateCache(delegateCache, cacheQueryResults, incomingChanges)));
    }

    private static <ID, R> Function<Map<ID, List<R>>, Mono<?>> emptyOr(Function<Map<ID, List<R>>, Mono<?>> mappingFunction) {
        return map -> isEmpty(map) ? just(of()) : mappingFunction.apply(map);
    }

    private static <ID, R> BiFunction<Iterable<ID>, FetchFunction<ID, R>, Mono<Map<ID, List<R>>>> emptyOr(
            BiFunction<Iterable<ID>, FetchFunction<ID, R>, Mono<Map<ID, List<R>>>> mappingFunction) {

        return (ids, fetchFunction) -> isEmpty(ids) ? just(of()) : mappingFunction.apply(ids, fetchFunction);
    }

    Mono<Map<ID, List<R>>> getAll(Iterable<ID> ids, FetchFunction<ID, R> fetchFunction);

    Mono<?> putAll(Map<ID, List<R>> map);

    Mono<?> removeAll(Map<ID, List<R>> map);

    default Mono<?> updateAll(Map<ID, List<R>> mapToAdd, Map<ID, List<R>> mapToRemove) {
        return putAll(mapToAdd).then(removeAll(mapToRemove));
    }

    interface FetchFunction<ID, R> extends Function<Iterable<? extends ID>, Mono<Map<ID, List<R>>>> {
    }
}
