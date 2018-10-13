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
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.*;

public interface QueryUtils {

    static <ID, R, IDC extends Collection<ID>, RC extends Collection<R>, EX extends Throwable>
    Map<ID, R> queryOneToOne(IDC ids,
                             CheckedFunction1<IDC, RC, EX> queryFunction,
                             Function<R, ID> idExtractorFromQueryResults) throws EX {

        return queryOneToOne(ids, queryFunction, idExtractorFromQueryResults, id -> null);
    }

    static <ID, R, IDC extends Collection<ID>, RC extends Collection<R>, EX extends Throwable>
    Map<ID, R> queryOneToOne(IDC ids,
                             CheckedFunction1<IDC, RC, EX> queryFunction,
                             Function<R, ID> idExtractorFromQueryResults,
                             Function<ID, R> defaultResultProvider) throws EX {

        return query(ids, queryFunction, defaultResultProvider, toMap(idExtractorFromQueryResults, identity()));
    }

    static <ID, R, IDC extends Collection<ID>, EX extends Throwable>
    Map<ID, List<R>> queryOneToManyAsList(IDC ids,
                                          CheckedFunction1<IDC, List<R>, EX> queryFunction,
                                          Function<R, ID> idExtractorFromQueryResults) throws EX {

        return queryOneToMany(ids, queryFunction, idExtractorFromQueryResults, ArrayList::new);
    }

    static <ID, R, IDC extends Collection<ID>, EX extends Throwable>
    Map<ID, Set<R>> queryOneToManyAsSet(IDC ids,
                                        CheckedFunction1<IDC, Set<R>, EX> queryFunction,
                                        Function<R, ID> idExtractorFromQueryResults) throws EX {

        return queryOneToMany(ids, queryFunction, idExtractorFromQueryResults, HashSet::new);
    }

    static <ID, R, IDC extends Collection<ID>, RC extends Collection<R>, EX extends Throwable>
    Map<ID, RC> queryOneToMany(IDC ids,
                               CheckedFunction1<IDC, RC, EX> queryFunction,
                               Function<R, ID> idExtractorFromQueryResults,
                               Supplier<RC> collectionFactory) throws EX {

        return query(ids, queryFunction, id -> collectionFactory.get(),
                groupingBy(idExtractorFromQueryResults, toCollection(collectionFactory)));
    }

    /**
     * @param ids                   The collection of ids to pass to the {@code queryFunction}
     * @param queryFunction         The query function to call (rest call, spring data repository method call, etc.)
     * @param defaultResultProvider The default value to generate if no result for a specific id
     *                              passed to the {@code queryFunction}
     *                              e.g. an empty collection, a default value, an empty string, etc.
     * @param mapCollector          The collector used to collect the stream of results returned by {@code queryFunction}.
     *                              It will transform a stream of results to a {@code Map<ID, V>}
     * @param <V>                   Type of each value representing the result from {@code queryFunction} associated with each ID, will map
     *                              to either {@code <R>} when called from {@code queryOneToOne}
     *                              or {@code <D>} when called from {@code queryOneToMany} i.e. a collection of {@code <R>}.
     *                              This is conceptually a union type {@code <R> | <D>}
     * @param <ID>                  Type of the ids passed to {@code queryFunction} e.g. {@code Long}, {@code String}
     * @param <R>                   Type of individual results returned from the queryFunction
     * @param <IDC>                 Type of the {@link Collection} containing the ids of type {@code <ID>}
     *                              e.g. {@code List<Long>}, {@code Set<String>}, etc.
     * @param <RC>                  Type of the {@link Collection} containing the results of type {@code <R>}
     *                              e.g. {@code List<Customer>}, {@code Set<Order>}, etc.
     * @param <EX>                  Type of the exception that can be thrown by the {@code queryFunction}
     * @return A {@link Map} of the results from invoking the {@code queryFunction}
     * with key = correlation ID, value = result associated with ID
     * @throws EX If {@code queryFunction} throws an exception, that exception will be propagated to the caller of this method
     */
    static <V, ID, R, IDC extends Collection<ID>, RC extends Collection<R>, EX extends Throwable>
    Map<ID, V> query(IDC ids,
                     CheckedFunction1<IDC, RC, EX> queryFunction,
                     Function<ID, V> defaultResultProvider,
                     Collector<R, ?, Map<ID, V>> mapCollector) throws EX {

        Map<ID, V> resultMap = safeApply(ids, queryFunction)
                .collect(mapCollector);

        if (isSafeEqual(resultMap, ids, Map::size, Collection::size))
            return resultMap;

        Set<ID> idsFromQueryResult = resultMap.keySet();

        // defaultResultProvider can provide a null value, so we cannot use a Collector here
        // as it would throw a NullPointerException
        ids.stream()
                .filter(not(idsFromQueryResult::contains))
                .forEach(id -> resultMap.put(id, defaultResultProvider.apply(id)));

        return resultMap;
    }

    /**
     *
     *
     * @param coll          The list of arguments to pass to the queryFunction
     *                      e.g. {@code List<Long>} for passing a list of IDs to query a database
     * @param queryFunction The function to apply the list of arguments
     * @param <T>
     * @param <R>
     * @param <C>
     * @param <RC>
     * @param <EX>
     * @return
     */
    static <T, R, C extends Collection<? extends T>, RC extends Collection<? extends R>, EX extends Throwable>
    Stream<? extends R> safeApply(C coll, CheckedFunction1<C, RC, EX> queryFunction) {
        Optional<RC> resultsFromQuery = Optional.ofNullable(coll)
                .filter(not(Collection::isEmpty))
                .map(unchecked(queryFunction));

        return resultsFromQuery.stream()
                .flatMap(Collection::stream)
                .filter(Objects::nonNull);
    }

    @SafeVarargs
    private static <K, V> Map<K, V> merge(Map<K, V>... maps) {
        return Stream.of(maps)
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .collect(toMap(Entry::getKey, Entry::getValue));
    }
}
