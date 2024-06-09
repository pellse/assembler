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

import java.util.function.BiFunction;
import java.util.function.Function;

import static io.github.pellse.assembler.caching.Cache.adapterCache;
import static io.github.pellse.util.ObjectUtils.then;
import static io.github.pellse.util.collection.CollectionUtils.transformMap;

public interface MapperCacheFactory {

    static <ID, EID, R, RRC, CTX extends CacheContext<ID, EID, R, RRC>> CacheTransformer<ID, EID, R, RRC, CTX> mapper(Function<CTX, BiFunction<ID, RRC, RRC>> mappingFunction) {
        return cacheFactory -> mapper(cacheFactory, mappingFunction);
    }

    static <ID, EID, R, RRC, CTX extends CacheContext<ID, EID, R, RRC>> CacheFactory<ID, EID, R, RRC, CTX> mapper(CacheFactory<ID, EID, R, RRC, CTX> cacheFactory, Function<CTX, BiFunction<ID, RRC, RRC>> mappingFunction) {
        return context -> then(cacheFactory.create(context), delegateCache -> adapterCache(
                delegateCache::getAll,
                (ids, fetchFunction) -> delegateCache.computeAll(ids, idList -> fetchFunction.apply(idList).map(m -> transformMap(m, mappingFunction.apply(context)))),
                map -> delegateCache.putAll(transformMap(map, mappingFunction.apply(context))),
                delegateCache::removeAll,
                (mapToAdd, mapToRemove) -> delegateCache.updateAll(transformMap(mapToAdd, mappingFunction.apply(context)), mapToRemove)
        ));
    }
}