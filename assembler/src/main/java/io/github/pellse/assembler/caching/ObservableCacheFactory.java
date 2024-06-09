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

import io.github.pellse.assembler.caching.CacheFactory.CacheTransformer;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.github.pellse.assembler.caching.Cache.adapterCache;

public interface ObservableCacheFactory {

    static <ID, EID, R, RRC, CTX extends CacheContext<ID, EID, R, RRC>> CacheTransformer<ID, EID, R, RRC, CTX> observableCache(
            Consumer<Map<ID, RRC>> onGetAll,
            Consumer<Map<ID, RRC>> onComputeAll,
            Consumer<Map<ID, RRC>> onPutAll,
            Consumer<Map<ID, RRC>> onRemoveAll,
            BiConsumer<Map<ID, RRC>, Map<ID, RRC>> onUpdateAll) {

        return cacheFactory -> cacheContext -> {

            final var cache = cacheFactory.create(cacheContext);

            return adapterCache(
                    ids -> cache.getAll(ids).transform(call(onGetAll)),
                    (ids, fetchFunction) -> cache.computeAll(ids, fetchFunction).transform(call(onComputeAll)),
                    map -> cache.putAll(map).transform(call(map, onPutAll)),
                    map -> cache.removeAll(map).transform(call(map, onRemoveAll)),
                    (mapToAdd, mapToRemove) -> cache.updateAll(mapToAdd, mapToRemove).transform(call(mapToAdd, mapToRemove, onUpdateAll))
            );
        };
    }

    static <T> Function<Mono<T>, Mono<T>> call(Consumer<T> consumer) {
        return mono -> consumer != null ? mono.doOnNext(consumer) : mono;
    }

    static <T, U> Function<Mono<T>, Mono<T>> call(U value, Consumer<U> consumer) {
        return mono -> consumer != null ? mono.doOnNext(__ -> consumer.accept(value)) : mono;
    }

    static <T, U, V> Function<Mono<T>, Mono<T>> call(U value1, V value2, BiConsumer<U, V> consumer) {
        return mono -> consumer != null ? mono.doOnNext(__ -> consumer.accept(value1, value2)) : mono;
    }
}
