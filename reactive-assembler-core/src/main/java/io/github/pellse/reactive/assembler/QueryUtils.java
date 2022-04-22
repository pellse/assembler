/*
 * Copyright 2018 Sebastien Pelletier
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

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static io.github.pellse.reactive.assembler.MapFactory.defaultMapFactory;
import static io.github.pellse.util.ObjectUtils.isSafeEqual;
import static java.util.Objects.*;
import static java.util.function.Function.identity;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.*;

public interface QueryUtils {

    @NotNull
    static <ID, IDC extends Collection<ID>, R>
    Mono<Map<ID, R>> queryOneToOne(IDC ids,
                                   Function<IDC, Publisher<R>> queryFunction,
                                   Function<R, ID> idExtractorFromQueryResults) {

        return queryOneToOne(ids, queryFunction, idExtractorFromQueryResults, defaultMapFactory());
    }

    @NotNull
    static <ID, IDC extends Collection<ID>, R>
    Mono<Map<ID, R>> queryOneToOne(IDC ids,
                                   Function<IDC, Publisher<R>> queryFunction,
                                   Function<R, ID> idExtractorFromQueryResults,
                                   MapFactory<ID, R> mapFactory) {

        return queryOneToOne(ids, queryFunction, idExtractorFromQueryResults, id -> null, mapFactory);
    }

    @NotNull
    static <ID, IDC extends Collection<ID>, R>
    Mono<Map<ID, R>> queryOneToOne(IDC ids,
                                   Function<IDC, Publisher<R>> queryFunction,
                                   Function<R, ID> idExtractorFromQueryResults,
                                   Function<ID, R> defaultResultProvider) {

        return queryOneToOne(ids, queryFunction, idExtractorFromQueryResults, defaultResultProvider, defaultMapFactory());
    }

    @NotNull
    static <ID, IDC extends Collection<ID>, R>
    Mono<Map<ID, R>> queryOneToOne(IDC ids,
                                   Function<IDC, Publisher<R>> queryFunction,
                                   Function<R, ID> idExtractorFromQueryResults,
                                   Function<ID, R> defaultResultProvider,
                                   MapFactory<ID, R> mapFactory) {

        return query(ids, queryFunction, defaultResultProvider, toMap(idExtractorFromQueryResults, identity(), (u1, u2) -> u1, toSupplier(ids, mapFactory)));
    }

    @NotNull
    static <ID, IDC extends Collection<ID>, R>
    Mono<Map<ID, List<R>>> queryOneToManyAsList(IDC ids,
                                                Function<IDC, Publisher<R>> queryFunction,
                                                Function<R, ID> idExtractorFromQueryResults) {

        return queryOneToManyAsList(ids, queryFunction, idExtractorFromQueryResults, defaultMapFactory());
    }

    @NotNull
    static <ID, IDC extends Collection<ID>, R>
    Mono<Map<ID, List<R>>> queryOneToManyAsList(IDC ids,
                                                Function<IDC, Publisher<R>> queryFunction,
                                                Function<R, ID> idExtractorFromQueryResults,
                                                MapFactory<ID, List<R>> mapFactory) {

        return queryOneToMany(ids, queryFunction, idExtractorFromQueryResults, ArrayList::new, mapFactory);
    }

    @NotNull
    static <ID, IDC extends Collection<ID>, R>
    Mono<Map<ID, Set<R>>> queryOneToManyAsSet(IDC ids,
                                              Function<IDC, Publisher<R>> queryFunction,
                                              Function<R, ID> idExtractorFromQueryResults) {

        return queryOneToManyAsSet(ids, queryFunction, idExtractorFromQueryResults, defaultMapFactory());
    }

    @NotNull
    static <ID, IDC extends Collection<ID>, R>
    Mono<Map<ID, Set<R>>> queryOneToManyAsSet(IDC ids,
                                              Function<IDC, Publisher<R>> queryFunction,
                                              Function<R, ID> idExtractorFromQueryResults,
                                              MapFactory<ID, Set<R>> mapFactory) {

        return queryOneToMany(ids, queryFunction, idExtractorFromQueryResults, HashSet::new, mapFactory);
    }

    @NotNull
    static <ID, IDC extends Collection<ID>, R, RC extends Collection<R>>
    Mono<Map<ID, RC>> queryOneToMany(IDC ids,
                                     Function<IDC, Publisher<R>> queryFunction,
                                     Function<R, ID> idExtractorFromQueryResults,
                                     Supplier<RC> collectionFactory) {

        return queryOneToMany(ids, queryFunction, idExtractorFromQueryResults, collectionFactory, defaultMapFactory());
    }

    @NotNull
    static <ID, IDC extends Collection<ID>, R, RC extends Collection<R>>
    Mono<Map<ID, RC>> queryOneToMany(IDC ids,
                                     Function<IDC, Publisher<R>> queryFunction,
                                     Function<R, ID> idExtractorFromQueryResults,
                                     Supplier<RC> collectionFactory,
                                     MapFactory<ID, RC> mapFactory) {

        return query(ids, queryFunction, id -> collectionFactory.get(),
                groupingBy(idExtractorFromQueryResults, toSupplier(ids, mapFactory), toCollection(collectionFactory)));
    }

    /**
     * @param ids                   The collection of ids to pass to the {@code queryFunction}
     * @param queryFunction         The query function to call (rest call, spring data repository method call, etc.)
     * @param defaultResultProvider The default value to generate if no result for a specific id
     *                              passed to the {@code queryFunction}
     *                              e.g. an empty collection, a default value, an empty string, etc.
     * @param mapCollector          The collector used to collect the stream of results returned by {@code queryFunction}.
     *                              It will transform a stream of results to a {@code Map<ID, V>}
     * @param <ID>                  Type of the ids passed to {@code queryFunction} e.g. {@code Long}, {@code String}
     * @param <IDC>                 Type of the {@link Collection} containing the ids of type {@code <ID>}
     *                              e.g. {@code List<Long>}, {@code Set<String>}, etc.
     *                              e.g. {@code List<Customer>}, {@code Set<Order>}, etc.
     * @param <R>                   Type of individual results returned from the queryFunction
     * @param <RRC>                 Type of each value representing the result from {@code queryFunction} associated with each ID, will map
     *                              to either {@code <R>} when called from {@code queryOneToOne}
     *                              or {@code <RC>} when called from {@code queryOneToMany} i.e. a collection of {@code <R>}.
     *                              This is conceptually a union type {@code <R> | <RC>}
     * @return A {@link Map} of the results from invoking the {@code queryFunction}
     * with key = correlation ID, value = result associated with ID
     */
    @NotNull
    static <ID, IDC extends Collection<ID>, R, RRC>
    Mono<Map<ID, RRC>> query(IDC ids,
                           Function<IDC, Publisher<R>> queryFunction,
                           Function<ID, RRC> defaultResultProvider,
                           Collector<R, ?, Map<ID, RRC>> mapCollector) {

        return safeApply(ids, queryFunction)
                .collect(mapCollector)
                .flatMap(map -> toResultMap(ids, map, defaultResultProvider));
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
    private static <ID, IDC extends Collection<ID>, RRC>
    Mono<Map<ID, RRC>> toResultMap(IDC ids, Map<ID, RRC> map, Function<ID, RRC> defaultResultProvider) {
        return Mono.just(isSafeEqual(map, Map::size, ids, Collection::size) ? map : initializeResultMap(ids, map, defaultResultProvider));
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
    private static <ID, IDC extends Collection<ID>, R>
    Supplier<Map<ID, R>> toSupplier(IDC ids, MapFactory<ID, R> mapFactory) {
        MapFactory<ID, R> actualMapFactory = requireNonNullElseGet(mapFactory, MapFactory::defaultMapFactory);
        return () -> actualMapFactory.apply(ids != null ? ids.size() : 0);
    }
}
