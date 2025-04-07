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

import static io.github.pellse.assembler.caching.AdapterCache.adapterCache;

public interface DeferCacheFactory {

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheTransformer<ID, R, RRC, CTX> defer() {
        return DeferCacheFactory::defer;
    }

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheFactory<ID, R, RRC, CTX> defer(CacheFactory<ID, R, RRC, CTX> cacheFactory) {
        return context -> defer(cacheFactory.create(context));
    }

    static <ID, RRC> Cache<ID, RRC> defer(Cache<ID, RRC> delegateCache) {
        return adapterCache(
                ids -> Mono.defer(() -> delegateCache.getAll(ids)),
                (ids, fetchFunction) -> Mono.defer(() -> delegateCache.computeAll(ids, fetchFunction)),
                map -> Mono.defer(() -> delegateCache.putAll(map)),
                map -> Mono.defer(() -> delegateCache.removeAll(map)),
                (mapToAdd, mapToRemove) -> Mono.defer(() -> delegateCache.updateAll(mapToAdd, mapToRemove)));
    }
}
