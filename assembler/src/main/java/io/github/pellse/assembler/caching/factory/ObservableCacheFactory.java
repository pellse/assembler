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

import io.github.pellse.assembler.caching.Cache;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

@Deprecated
public interface ObservableCacheFactory {

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheTransformer<ID, R, RRC, CTX> observableCache(
            Consumer<Map<ID, RRC>> onGetAll,
            Consumer<Map<ID, RRC>> onComputeAll,
            Consumer<Map<ID, RRC>> onPutAll,
            Consumer<Map<ID, RRC>> onRemoveAll,
            BiConsumer<Map<ID, RRC>, Map<ID, RRC>> onUpdateAll) {

        return cacheFactory -> cacheContext -> {

            final var cache = cacheFactory.create(cacheContext);

            return new Cache<>() {

                @Override
                public Mono<Map<ID, RRC>> getAll(Iterable<ID> ids) {
                    return cache.getAll(ids).transform(call(onGetAll));
                }

                @Override
                public Mono<Map<ID, RRC>> computeAll(Iterable<ID> ids, FetchFunction<ID, RRC> fetchFunction) {
                    return cache.computeAll(ids, fetchFunction).transform(call(onComputeAll));
                }

                @Override
                public Mono<?> putAll(Map<ID, RRC> map) {
                    return cache.putAll(map).transform(call(map, onPutAll));
                }

                @Override
                public Mono<?> removeAll(Map<ID, RRC> map) {
                    return cache.removeAll(map).transform(call(map, onRemoveAll));
                }

                @Override
                public Mono<?> updateAll(Map<ID, RRC> mapToAdd, Map<ID, RRC> mapToRemove) {
                    return cache.updateAll(mapToAdd, mapToRemove).transform(call(mapToAdd, mapToRemove, onUpdateAll));
                }
            };
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
