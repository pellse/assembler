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

package io.github.pellse.reactive.assembler;

import io.github.pellse.util.collection.CollectionUtil;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.github.pellse.util.ObjectUtils.isSafeEqual;
import static java.util.Objects.*;
import static java.util.function.Predicate.not;
import static reactor.core.publisher.Flux.fromIterable;

public interface QueryUtils {

    static <ID, IDC extends Collection<ID>, R> Function<IDC, Publisher<R>> toPublisher(Function<IDC, Iterable<R>> queryFunction) {
        return ids -> fromIterable(queryFunction.apply(ids));
    }

    @NotNull
    static <T, R, C extends Iterable<? extends T>>
    Flux<R> safeApply(C coll, Function<C, Publisher<R>> queryFunction) {
        requireNonNull(queryFunction, "queryFunction cannot be null");

        return Mono.just(coll)
                .filter(CollectionUtil::isNotEmpty)
                .flatMapMany(queryFunction);
    }

    @NotNull
    static <ID, IDC extends Collection<ID>, RRC>
    Map<ID, RRC> toResultMap(IDC ids, Map<ID, RRC> map, Function<ID, RRC> defaultResultProvider) {
        return isSafeEqual(map, Map::size, ids, Collection::size) ? map : initializeResultMap(ids, map, defaultResultProvider);
    }

    @NotNull
    private static <ID, IDC extends Collection<ID>, RRC>
    Map<ID, RRC> initializeResultMap(IDC ids, Map<ID, RRC> resultMap, Function<ID, RRC> defaultResultProvider) {
        Function<ID, RRC> resultProvider = requireNonNullElse(defaultResultProvider, id -> null);
        Set<ID> idsFromQueryResult = resultMap.keySet();
        Map<ID, RRC> resultMapCopy = new HashMap<>(resultMap);

        // defaultResultProvider can provide a null value, so we cannot use a Collector here
        // as it would throw a NullPointerException
        ids.stream()
                .filter(not(idsFromQueryResult::contains))
                .forEach(id -> resultMapCopy.put(id, resultProvider.apply(id)));

        return resultMapCopy;
    }

    @NotNull
    @Contract(pure = true)
    static <ID, R>
    Supplier<Map<ID, R>> toSupplier(int initialCapacity, MapFactory<ID, R> mapFactory) {
        MapFactory<ID, R> actualMapFactory = requireNonNullElseGet(mapFactory, MapFactory::defaultMapFactory);
        return () -> actualMapFactory.apply(initialCapacity);
    }
}
