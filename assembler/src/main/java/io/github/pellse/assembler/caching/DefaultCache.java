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

import io.github.pellse.assembler.caching.Cache.FetchFunction;
import io.github.pellse.assembler.caching.factory.CacheContext;
import io.github.pellse.assembler.caching.factory.CacheFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.github.pellse.assembler.caching.AdapterCache.adapterCache;
import static io.github.pellse.assembler.caching.factory.CacheFactory.toMono;
import static io.github.pellse.util.ObjectUtils.also;
import static io.github.pellse.util.collection.CollectionUtils.*;
import static io.github.pellse.util.reactive.ReactiveUtils.createSinkMap;
import static io.github.pellse.util.reactive.ReactiveUtils.resolve;
import static java.util.Optional.ofNullable;

public interface DefaultCache {

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheFactory<ID, R, RRC, CTX> cache() {

        final var delegateMap = new ConcurrentHashMap<ID, Sinks.One<RRC>>();

        final Function<Iterable<ID>, Mono<Map<ID, RRC>>> getAll = ids -> resolve(readAll(ids, delegateMap, Sinks.One::asMono));

        final BiFunction<Iterable<ID>, FetchFunction<ID, RRC>, Mono<Map<ID, RRC>>> computeAll = (ids, fetchFunction) -> {
            final var cachedEntitiesMap = readAll(ids, delegateMap, Sinks.One::asMono);
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
                    .flatMap(__ -> resolve(mergeMaps(cachedEntitiesMap, transformMapValues(sinkMap, Sinks.One::asMono))));
        };

        Function<Map<ID, RRC>, Mono<?>> putAll = toMono(map -> also(createSinkMap(map.keySet()), delegateMap::putAll)
                .forEach((id, sink) -> sink.tryEmitValue(map.get(id))));

        Function<Map<ID, RRC>, Mono<?>> removeAll = toMono(map -> also(map.keySet(), ids -> delegateMap.keySet().removeAll(ids))
                .forEach(id -> ofNullable(delegateMap.get(id)).ifPresent(Sinks.One::tryEmitEmpty)));

        return cache(getAll, computeAll, putAll, removeAll);
    }

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheFactory<ID, R, RRC, CTX> cache(
            Function<Iterable<ID>, Mono<Map<ID, RRC>>> getAll,
            BiFunction<Iterable<ID>, FetchFunction<ID, RRC>, Mono<Map<ID, RRC>>> computeAll,
            Function<Map<ID, RRC>, Mono<?>> putAll,
            Function<Map<ID, RRC>, Mono<?>> removeAll) {

        return __ -> adapterCache(getAll, computeAll, putAll, removeAll);
    }
}
