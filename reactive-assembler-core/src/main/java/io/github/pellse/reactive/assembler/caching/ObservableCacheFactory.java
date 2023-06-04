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

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.github.pellse.reactive.assembler.caching.Cache.adapterCache;
import static io.github.pellse.util.ObjectUtils.then;

public interface ObservableCacheFactory {

    static <ID, R, RRC> Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> observableCache(
            Consumer<Map<ID, List<R>>> getAllCallback,
            Consumer<Map<ID, List<R>>> putAllCallback,
            Consumer<Map<ID, List<R>>> removeAllCallback,
            BiConsumer<Map<ID, List<R>>, Map<ID, List<R>>> updateAllCallback) {

        return cacheFactory -> observableCache(cacheFactory, getAllCallback, putAllCallback, removeAllCallback, updateAllCallback);
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> observableCache(
            CacheFactory<ID, R, RRC> delegateCacheFactory,
            Consumer<Map<ID, List<R>>> getAllCallback,
            Consumer<Map<ID, List<R>>> putAllCallback,
            Consumer<Map<ID, List<R>>> removeAllCallback,
            BiConsumer<Map<ID, List<R>>, Map<ID, List<R>>> updateAllCallback) {

        return context -> then(delegateCacheFactory.create(context), cache -> adapterCache(
                (ids, computeIfAbsent) -> then(cache.getAll(ids, computeIfAbsent),
                        mono -> getAllCallback != null ? mono.doOnNext(getAllCallback) : mono),
                map -> then(cache.putAll(map),
                        mono -> putAllCallback != null ? mono.doOnNext(__ -> putAllCallback.accept(map)) : mono),
                map -> then(cache.removeAll(map),
                        mono -> removeAllCallback != null ? mono.doOnNext(__ -> removeAllCallback.accept(map)) : mono),
                (mapToAdd, mapToRemove) -> then(cache.updateAll(mapToAdd, mapToRemove),
                        mono -> updateAllCallback != null ? mono.doOnNext(__ -> updateAllCallback.accept(mapToAdd, mapToRemove)) : mono)));
    }
}
