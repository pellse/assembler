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
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.github.pellse.util.ObjectUtils.then;
import static io.github.pellse.util.collection.CollectionUtils.*;
import static java.util.Map.of;
import static java.util.Optional.ofNullable;
import static reactor.core.publisher.Mono.*;

public interface Cache<ID, RRC> {

    Mono<Map<ID, RRC>> getAll(Iterable<ID> ids);

    Mono<Map<ID, RRC>> computeAll(Iterable<ID> ids, FetchFunction<ID, RRC> fetchFunction);

    Mono<?> putAll(Map<ID, RRC> map);

    Mono<?> removeAll(Map<ID, RRC> map);

    default Mono<?> updateAll(Map<ID, RRC> mapToAdd, Map<ID, RRC> mapToRemove) {
        return putAll(mapToAdd).then(removeAll(mapToRemove));
    }

    @FunctionalInterface
    interface CacheUpdater<ID, RRC> {
        Mono<?> updateCache(Cache<ID, RRC> cache, Map<ID, RRC> existingCacheItems, Map<ID, RRC> incomingChanges);
    }

    @FunctionalInterface
    interface FetchFunction<ID, RRC> extends Function<Iterable<? extends ID>, Mono<Map<ID, RRC>>> {
    }

    static <ID, RRC> Cache<ID, RRC> adapterCache(
            Function<Iterable<ID>, Mono<Map<ID, RRC>>> getAll,
            BiFunction<Iterable<ID>, FetchFunction<ID, RRC>, Mono<Map<ID, RRC>>> computeAll,
            Function<Map<ID, RRC>, Mono<?>> putAll,
            Function<Map<ID, RRC>, Mono<?>> removeAll) {

        return adapterCache(getAll, computeAll, putAll, removeAll, null);
    }

    static <ID, RRC> Cache<ID, RRC> adapterCache(
            Function<Iterable<ID>, Mono<Map<ID, RRC>>> getAll,
            BiFunction<Iterable<ID>, FetchFunction<ID, RRC>, Mono<Map<ID, RRC>>> computeAll,
            Function<Map<ID, RRC>, Mono<?>> putAll,
            Function<Map<ID, RRC>, Mono<?>> removeAll,
            BiFunction<Map<ID, RRC>, Map<ID, RRC>, Mono<?>> updateAll) {

        return new Cache<>() {

            @Override
            public Mono<Map<ID, RRC>> getAll(Iterable<ID> ids) {
                return getAll.apply(ids);
            }

            @Override
            public Mono<Map<ID, RRC>> computeAll(Iterable<ID> ids, FetchFunction<ID, RRC> fetchFunction) {
                return computeAll.apply(ids, fetchFunction);
            }

            @Override
            public Mono<?> putAll(Map<ID, RRC> map) {
                return putAll.apply(map);
            }

            @Override
            public Mono<?> removeAll(Map<ID, RRC> map) {
                return removeAll.apply(map);
            }

            @Override
            public Mono<?> updateAll(Map<ID, RRC> mapToAdd, Map<ID, RRC> mapToRemove) {
                return ofNullable(updateAll)
                        .orElse(Cache.super::updateAll)
                        .apply(mapToAdd, mapToRemove);
            }
        };
    }

    static <ID, RRC> Cache<ID, RRC> optimizedCache(Cache<ID, RRC> delegateCache) {
        return adapterCache(
                emptyOr(delegateCache::getAll),
                emptyOr(delegateCache::computeAll),
                emptyMapOr(delegateCache::putAll),
                emptyMapOr(delegateCache::removeAll),
                emptyMapOr(delegateCache::updateAll)
        );
    }

    static <ID, R> Cache<ID, R> oneToOneCache(Cache<ID, R> delegateCache) {
        return optimizedCache(delegateCache);
    }

    static <ID,  EID, R, RC extends Collection<R>> Cache<ID, RC> oneToManyCache(
            OneToManyContext<?, ?, ID, EID, R, RC> ctx,
            Cache<ID, RC> delegateCache) {

        final var optimizedCache = optimizedCache(delegateCache);

        return adapterCache(
                optimizedCache::getAll,
                optimizedCache::computeAll,
                applyMergeStrategy(
                        optimizedCache,
                        (existingCacheItems, incomingChanges) -> ctx.mapMerger().apply(existingCacheItems, incomingChanges),
                        Cache::putAll),
                applyMergeStrategy(
                        optimizedCache,
                        (cache, existingCacheItems, incomingChanges) ->
                                then(subtractFromMap(incomingChanges, existingCacheItems, ctx.idResolver(), ctx.collectionFactory()),
                                        updatedMap -> cache.updateAll(updatedMap, diff(existingCacheItems, updatedMap))))
//                (incomingChangesToAdd, incomingChangesToRemove) -> {
//                    delegateCache.getAll(Stream.concat(incomingChangesToAdd.keySet().stream(), incomingChangesToRemove.keySet().stream()).distinct().toList())
//                            .flatMap(existingCacheItems -> )
//
//
//                    return just(of());
//                }
        );
    }

    private static <ID, R, RC extends Collection<R>> Function<Map<ID, RC>, Mono<?>> applyMergeStrategy(
            Cache<ID, RC> delegateCache,
            MergeStrategy<ID, RC> mergeStrategy,
            BiFunction<Cache<ID, RC>, Map<ID, RC>, Mono<?>> cacheUpdater) {

        return applyMergeStrategy(
                delegateCache,
                (cache, existingCacheItems, incomingChanges) ->
                        cacheUpdater.apply(cache, mergeStrategy.merge(existingCacheItems, incomingChanges)));
    }

    private static <ID, R, RC extends Collection<R>> Function<Map<ID, RC>, Mono<?>> applyMergeStrategy(
            Cache<ID, RC> delegateCache,
            CacheUpdater<ID, RC> cacheUpdater) {

        return incomingChanges -> isEmpty(incomingChanges) ? just(of()) : delegateCache.getAll(incomingChanges.keySet())
                .flatMap(existingCacheItems -> cacheUpdater.updateCache(delegateCache, existingCacheItems, incomingChanges));
    }

    private static <ID, RRC> Function<Iterable<ID>, Mono<Map<ID, RRC>>> emptyOr(Function<Iterable<ID>, Mono<Map<ID, RRC>>> mappingFunction) {
        return ids -> isEmpty(ids) ? just(of()) : mappingFunction.apply(ids);
    }

    private static <ID, RRC> BiFunction<Iterable<ID>, FetchFunction<ID, RRC>, Mono<Map<ID, RRC>>> emptyOr(
            BiFunction<Iterable<ID>, FetchFunction<ID, RRC>, Mono<Map<ID, RRC>>> mappingFunction) {

        return (ids, fetchFunction) -> isEmpty(ids) ? just(of()) : mappingFunction.apply(ids, fetchFunction);
    }

    private static <ID, RRC> Function<Map<ID, RRC>, Mono<?>> emptyMapOr(Function<Map<ID, RRC>, Mono<?>> mappingFunction) {
        return map -> isEmpty(map) ? just(of()) : mappingFunction.apply(map);
    }

    private static <ID, RRC> BiFunction<Map<ID, RRC>, Map<ID, RRC>, Mono<?>> emptyMapOr(BiFunction<Map<ID, RRC>, Map<ID, RRC>, Mono<?>> mappingFunction) {
        return (map1, map2) -> isEmpty(map1) && isEmpty(map2) ? just(of()) : mappingFunction.apply(nullToEmptyMap(map1), nullToEmptyMap(map2));
    }
}
