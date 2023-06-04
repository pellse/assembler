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

package io.github.pellse.util.query;

import io.github.pellse.util.function.checked.CheckedFunction1;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static io.github.pellse.util.ObjectUtils.isSafeEqual;
import static io.github.pellse.util.function.checked.CheckedPredicate1.not;
import static io.github.pellse.util.function.checked.Unchecked.unchecked;
import static io.github.pellse.util.query.MapFactory.defaultMapFactory;
import static java.util.Objects.*;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.*;

public interface QueryUtils {

    static <T, ID, R, TC extends Collection<T>, RC extends Collection<R>, EX extends Throwable>
    Map<ID, R> queryOneToOne(TC entities,
                             CheckedFunction1<TC, RC, EX> queryFunction,
                             Function<T, ID> topLevelIdResolver,
                             Function<R, ID> idResolverFromQueryResults) throws EX {

        return queryOneToOne(entities, queryFunction, topLevelIdResolver, idResolverFromQueryResults, defaultMapFactory());
    }

    static <T, ID, R, TC extends Collection<T>, RC extends Collection<R>, EX extends Throwable>
    Map<ID, R> queryOneToOne(TC entities,
                             CheckedFunction1<TC, RC, EX> queryFunction,
                             Function<T, ID> topLevelIdResolver,
                             Function<R, ID> idResolverFromQueryResults,
                             MapFactory<ID, R> mapFactory) throws EX {

        return queryOneToOne(entities, queryFunction, topLevelIdResolver, idResolverFromQueryResults, id -> null, mapFactory);
    }

    static <T, ID, R, TC extends Collection<T>, RC extends Collection<R>, EX extends Throwable>
    Map<ID, R> queryOneToOne(TC entities,
                             CheckedFunction1<TC, RC, EX> queryFunction,
                             Function<T, ID> topLevelIdResolver,
                             Function<R, ID> idResolverFromQueryResults,
                             Function<ID, R> defaultResultProvider) throws EX {

        return queryOneToOne(entities, queryFunction, topLevelIdResolver, idResolverFromQueryResults, defaultResultProvider, defaultMapFactory());
    }

    static <T, ID, R, TC extends Collection<T>, RC extends Collection<R>, EX extends Throwable>
    Map<ID, R> queryOneToOne(TC entities,
                             CheckedFunction1<TC, RC, EX> queryFunction,
                             Function<T, ID> topLevelIdResolver,
                             Function<R, ID> idResolverFromQueryResults,
                             Function<ID, R> defaultResultProvider,
                             MapFactory<ID, R> mapFactory) throws EX {

        return query(entities, queryFunction, topLevelIdResolver, defaultResultProvider, toMap(idResolverFromQueryResults, identity(), (u1, u2) -> u1, toSupplier(entities, mapFactory)));
    }

    static <T, ID, R, TC extends Collection<T>, EX extends Throwable>
    Map<ID, List<R>> queryOneToManyAsList(TC entities,
                                          CheckedFunction1<TC, List<R>, EX> queryFunction,
                                          Function<T, ID> topLevelIdResolver,
                                          Function<R, ID> idResolverFromQueryResults) throws EX {

        return queryOneToManyAsList(entities, queryFunction, topLevelIdResolver, idResolverFromQueryResults, defaultMapFactory());
    }

    static <T, ID, R, TC extends Collection<T>, EX extends Throwable>
    Map<ID, List<R>> queryOneToManyAsList(TC entities,
                                          CheckedFunction1<TC, List<R>, EX> queryFunction,
                                          Function<T, ID> topLevelIdResolver,
                                          Function<R, ID> idResolverFromQueryResults,
                                          MapFactory<ID, List<R>> mapFactory) throws EX {

        return queryOneToMany(entities, queryFunction, topLevelIdResolver, idResolverFromQueryResults, ArrayList::new, mapFactory);
    }

    static <T, ID, R, TC extends Collection<T>, EX extends Throwable>
    Map<ID, Set<R>> queryOneToManyAsSet(TC entities,
                                        CheckedFunction1<TC, Set<R>, EX> queryFunction,
                                        Function<T, ID> topLevelIdResolver,
                                        Function<R, ID> idResolverFromQueryResults) throws EX {

        return queryOneToManyAsSet(entities, queryFunction, topLevelIdResolver, idResolverFromQueryResults, defaultMapFactory());
    }

    static <T, ID, R, TC extends Collection<T>, EX extends Throwable>
    Map<ID, Set<R>> queryOneToManyAsSet(TC entities,
                                        CheckedFunction1<TC, Set<R>, EX> queryFunction,
                                        Function<T, ID> topLevelIdResolver,
                                        Function<R, ID> idResolverFromQueryResults,
                                        MapFactory<ID, Set<R>> mapFactory) throws EX {

        return queryOneToMany(entities, queryFunction, topLevelIdResolver, idResolverFromQueryResults, HashSet::new, mapFactory);
    }

    static <T, ID, R, TC extends Collection<T>, RC extends Collection<R>, EX extends Throwable>
    Map<ID, RC> queryOneToMany(TC entities,
                               CheckedFunction1<TC, RC, EX> queryFunction,
                               Function<T, ID> topLevelIdResolver,
                               Function<R, ID> idResolverFromQueryResults,
                               Supplier<RC> collectionFactory) throws EX {

        return queryOneToMany(entities, queryFunction, topLevelIdResolver, idResolverFromQueryResults, collectionFactory, defaultMapFactory());
    }

    static <T, ID, R, TC extends Collection<T>, RC extends Collection<R>, EX extends Throwable>
    Map<ID, RC> queryOneToMany(TC entities,
                               CheckedFunction1<TC, RC, EX> queryFunction,
                               Function<T, ID> topLevelIdResolver,
                               Function<R, ID> idResolverFromQueryResults,
                               Supplier<RC> collectionFactory,
                               MapFactory<ID, RC> mapFactory) throws EX {

        return query(entities, queryFunction, topLevelIdResolver, id -> collectionFactory.get(),
                groupingBy(idResolverFromQueryResults, toSupplier(entities, mapFactory), toCollection(collectionFactory)));
    }

    /**
     * @param entities              The collection of entities to pass to the {@code queryFunction}
     * @param queryFunction         The query function to call (rest call, spring data repository method call, etc.)
     * @param defaultResultProvider The default value to generate if no result for a specific id
     *                              passed to the {@code queryFunction}
     *                              e.g. an empty collection, a default value, an empty string, etc.
     * @param mapCollector          The collector used to collect the stream of results returned by {@code queryFunction}.
     *                              It will transform a stream of results to a {@code Map<ID, V>}
     * @param <V>                   Type of each value representing the result from {@code queryFunction} associated with each ID, will map
     *                              to either {@code <R>} when called from {@code queryOneToOne}
     *                              or {@code <RC>} when called from {@code queryOneToMany} i.e. a collection of {@code <R>}.
     *                              This is conceptually a union type {@code <R> | <RC>}
     * @param <ID>                  Type of the ids passed to {@code queryFunction} e.g. {@code Long}, {@code String}
     * @param <R>                   Type of individual results returned from the queryFunction
     * @param <TC>                  Type of the {@link Collection} containing the ids of type {@code <ID>}
     *                              e.g. {@code List<Long>}, {@code Set<String>}, etc.
     * @param <RC>                  Type of the {@link Collection} containing the results of type {@code <R>}
     *                              e.g. {@code List<Customer>}, {@code Set<Order>}, etc.
     * @param <EX>                  Type of the exception that can be thrown by the {@code queryFunction}
     * @return A {@link Map} of the results from invoking the {@code queryFunction}
     * with key = correlation ID, value = result associated with ID
     * @throws EX If {@code queryFunction} throws an exception, that exception will be propagated to the caller of this method
     */
    static <T, V, ID, R, TC extends Collection<T>, RC extends Collection<R>, EX extends Throwable>
    Map<ID, V> query(
            TC entities,
            CheckedFunction1<TC, RC, EX> queryFunction,
            Function<T, ID> topLevelIdResolver,
            Function<ID, V> defaultResultProvider,
            Collector<R, ?, Map<ID, V>> mapCollector) throws EX {

        Map<ID, V> resultMap = safeApply(entities, queryFunction)
                .collect(mapCollector);

        if (isSafeEqual(resultMap, Map::size, entities, Collection::size))
            return resultMap;

        Function<ID, V> resultProvider = requireNonNullElse(defaultResultProvider, id -> null);

        Set<ID> idsFromQueryResult = resultMap.keySet();

        // defaultResultProvider can provide a null value, so we cannot use a Collector here
        // as it would throw a NullPointerException
        entities.stream()
                .map(topLevelIdResolver)
                .filter(not(idsFromQueryResult::contains))
                .forEach(id -> resultMap.put(id, resultProvider.apply(id)));

        return resultMap;
    }
    
    static <T, R, C extends Collection<? extends T>, RC extends Collection<? extends R>, EX extends Throwable>
    Stream<? extends R> safeApply(C coll, CheckedFunction1<C, RC, EX> queryFunction) {
        requireNonNull(queryFunction, "queryFunction cannot be null");

        Optional<RC> resultsFromQuery = Optional.ofNullable(coll)
                .filter(not(Collection::isEmpty))
                .map(unchecked(queryFunction));

        return resultsFromQuery.stream()
                .flatMap(Collection::stream)
                .filter(Objects::nonNull);
    }

    private static <T, ID, R, TC extends Collection<T>>
    Supplier<Map<ID, R>> toSupplier(TC entities, MapFactory<ID, R> mapFactory) {
        MapFactory<ID, R> mapSupplier = requireNonNullElseGet(mapFactory, MapFactory::defaultMapFactory);
        return () -> mapSupplier.apply(entities != null ? entities.size() : 0);
    }

    @SafeVarargs
    private static <K, V> Map<K, V> merge(Map<K, V>... maps) {
        return Stream.of(maps)
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .collect(toMap(Entry::getKey, Entry::getValue));
    }
}
