/*
 * Copyright 2017 Sebastien Pelletier
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

import static io.github.pellse.util.function.checked.CheckedPredicate1.not;
import static io.github.pellse.util.function.checked.Unchecked.unchecked;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.*;

public final class QueryUtils {

    private QueryUtils() {

    }

    public static <ID, R, IDC extends Collection<ID>, D extends Collection<R>, EX extends Throwable>
    Map<ID, R> queryOneToOne(IDC ids,
                             CheckedFunction1<IDC, D, EX> queryFunction,
                             Function<R, ID> idExtractorFromQueryResults) throws EX {

        return queryOneToOne(ids, queryFunction, idExtractorFromQueryResults, id -> null);
    }

    public static <ID, R, IDC extends Collection<ID>, D extends Collection<R>, EX extends Throwable>
    Map<ID, R> queryOneToOne(IDC ids,
                             CheckedFunction1<IDC, D, EX> queryFunction,
                             Function<R, ID> idExtractorFromQueryResults,
                             Function<ID, R> defaultResultProvider) throws EX {

        return query(ids, queryFunction, defaultResultProvider, toMap(idExtractorFromQueryResults, identity()));
    }

    public static <ID, R, IDC extends Collection<ID>, EX extends Throwable>
    Map<ID, List<R>> queryOneToManyAsList(IDC ids,
                                          CheckedFunction1<IDC, List<R>, EX> queryFunction,
                                          Function<R, ID> idExtractorFromQueryResults) throws EX {

        return queryOneToMany(ids, queryFunction, idExtractorFromQueryResults, ArrayList::new);
    }

    public static <ID, R, IDC extends Collection<ID>, EX extends Throwable>
    Map<ID, Set<R>> queryOneToManyAsSet(IDC ids,
                                        CheckedFunction1<IDC, Set<R>, EX> queryFunction,
                                        Function<R, ID> idExtractorFromQueryResults) throws EX {

        return queryOneToMany(ids, queryFunction, idExtractorFromQueryResults, HashSet::new);
    }

    public static <ID, R, IDC extends Collection<ID>, D extends Collection<R>, EX extends Throwable>
    Map<ID, D> queryOneToMany(IDC ids,
                              CheckedFunction1<IDC, D, EX> queryFunction,
                              Function<R, ID> idExtractorFromQueryResults,
                              Supplier<D> collectionFactory) throws EX {

        return query(ids, queryFunction, id -> collectionFactory.get(),
                groupingBy(idExtractorFromQueryResults, toCollection(collectionFactory)));
    }

    public static <T, ID, R, IDC extends Collection<ID>, D extends Collection<R>, EX extends Throwable>
    Map<ID, T> query(IDC ids,
                     CheckedFunction1<IDC, D, EX> queryFunction,
                     Function<ID, T> defaultResultProvider,
                     Collector<R, ?, Map<ID, T>> mapCollectorFactory) throws EX {

        Map<ID, T> resultMap = safeApply(ids, queryFunction)
                .collect(mapCollectorFactory);

        Set<ID> idsFromQueryResult = resultMap.keySet();

        // defaultResultProvider can provide a null value, so we cannot use a Collector here
        // as it would throw a NullPointerException
        ids.stream()
                .filter(not(idsFromQueryResult::contains))
                .forEach(id -> resultMap.put(id, defaultResultProvider.apply(id)));

        return resultMap;
    }

    public static <T, R, C extends Collection<? extends T>, D extends Collection<? extends R>, EX extends Throwable>
    Stream<? extends R> safeApply(C coll, CheckedFunction1<C, D, EX> queryFunction) throws EX {
        return Optional.ofNullable(coll)
                .filter(not(Collection::isEmpty))
                .map(unchecked(queryFunction))
                .stream()
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
