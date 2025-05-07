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
import io.github.pellse.assembler.caching.factory.CacheFactory.CacheTransformer;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.github.pellse.util.collection.CollectionUtils.transformMapValues;

public interface MapperCacheFactory {

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheTransformer<ID, R, RRC, CTX> mapper(Function<CTX, BiFunction<ID, RRC, RRC>> mappingFunction) {
        return cacheFactory -> mapper(cacheFactory, mappingFunction);
    }

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheFactory<ID, R, RRC, CTX> mapper(CacheFactory<ID, R, RRC, CTX> cacheFactory, Function<CTX, BiFunction<ID, RRC, RRC>> mappingFunction) {

        return context -> {

            final var delegateCache = cacheFactory.create(context);

            return new Cache<>() {

                @Override
                public Mono<Map<ID, RRC>> getAll(Iterable<ID> ids) {
                    return delegateCache.getAll(ids);
                }

                @Override
                public Mono<Map<ID, RRC>> computeAll(Iterable<ID> ids, FetchFunction<ID, RRC> fetchFunction) {
                    return delegateCache.computeAll(ids, idList -> fetchFunction.apply(idList).map(m -> transformMapValues(m, mappingFunction.apply(context))));
                }

                @Override
                public Mono<?> putAll(Map<ID, RRC> map) {
                    return delegateCache.putAll(transformMapValues(map, mappingFunction.apply(context)));
                }

                @Override
                public Mono<?> removeAll(Map<ID, RRC> map) {
                    return delegateCache.removeAll(map);
                }

                @Override
                public Mono<?> updateAll(Map<ID, RRC> mapToAdd, Map<ID, RRC> mapToRemove) {
                    return delegateCache.updateAll(transformMapValues(mapToAdd, mappingFunction.apply(context)), mapToRemove);
                }
            };
        };
    }
}