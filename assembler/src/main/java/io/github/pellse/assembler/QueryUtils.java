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

package io.github.pellse.assembler;

import io.github.pellse.util.collection.CollectionUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.github.pellse.assembler.RuleMapperSource.nullToEmptySource;
import static io.github.pellse.util.ObjectUtils.isSafeEqual;
import static io.github.pellse.util.collection.CollectionUtils.*;
import static java.util.Objects.*;
import static java.util.function.Predicate.not;
import static reactor.core.publisher.Flux.fromIterable;

public interface QueryUtils {

    static <T, TC extends Collection<T>, K, ID, EID, R, RRC, CTX extends RuleMapperContext<T, TC, K, ID, EID, R, RRC>> Function<Iterable<T>, Mono<Map<ID, RRC>>> buildQueryFunction(
            RuleMapperSource<T, TC, K, ID, EID, R, RRC, CTX> ruleMapperSource,
            CTX ctx) {

        final var queryFunction = nullToEmptySource(ruleMapperSource).apply(ctx);

        return entityList -> {
            var entities = translate(entityList, ctx.topLevelCollectionFactory());

            return safeApply(entities, queryFunction)
                    .collect(ctx.mapCollector().apply(entities.size()))
                    .map(map -> toResultMap(entities, map, ctx.outerIdResolver(), ctx.defaultResultProvider()));
        };
    }

    static <T, TC extends Collection<T>, R> Function<TC, Publisher<R>> toPublisher(Function<TC, Iterable<R>> queryFunction) {
        return entities -> fromIterable(queryFunction.apply(entities));
    }

    static <T, R, C extends Iterable<? extends T>>
    Flux<R> safeApply(C coll, Function<C, Publisher<R>> queryFunction) {

        requireNonNull(queryFunction, "queryFunction cannot be null");

        return Mono.just(coll)
                .filter(CollectionUtils::isNotEmpty)
                .flatMapMany(queryFunction);
    }

    static <T, ID, RRC> Map<ID, RRC> toResultMap(
            Collection<T> entities,
            Map<ID, RRC> map,
            Function<T, ID> topLevelIdResolver,
            Function<ID, RRC> defaultResultProvider) {

        return isSafeEqual(map, Map::size, entities, Collection::size)
                ? map
                : initializeResultMap(transform(entities, topLevelIdResolver), map, defaultResultProvider);
    }

    static <ID, RRC> Map<ID, RRC> initializeResultMap(Collection<ID> ids, Map<ID, RRC> resultMap, Function<ID, RRC> defaultResultProvider) {
        final Function<ID, RRC> resultProvider = requireNonNullElse(defaultResultProvider, id -> null);

        final Map<ID, RRC> resultLinkedHashMap = toLinkedHashMap(resultMap);
        final Set<ID> idsFromQueryResult = resultLinkedHashMap.keySet();
        final Map<ID, RRC> resultMapCopy = new LinkedHashMap<>(resultLinkedHashMap);

        // defaultResultProvider can provide a null value, so we cannot use a Collector here
        // as it would throw a NullPointerException
        ids.stream()
                .filter(not(idsFromQueryResult::contains))
                .forEach(id -> resultMapCopy.put(id, resultProvider.apply(id)));

        return resultMapCopy;
    }

    static <ID, R> Supplier<Map<ID, R>> toMapSupplier(int initialCapacity, MapFactory<ID, R> mapFactory) {

        final MapFactory<ID, R> actualMapFactory = requireNonNullElseGet(mapFactory, MapFactory::defaultMapFactory);
        return () -> actualMapFactory.apply(initialCapacity);
    }
}
