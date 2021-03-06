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

import io.github.pellse.util.function.checked.CheckedFunction1;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.github.pellse.reactive.assembler.QueryUtils.queryOneToMany;
import static io.github.pellse.reactive.assembler.QueryUtils.queryOneToOne;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.StreamSupport.stream;
import static reactor.core.publisher.Mono.from;

@FunctionalInterface
public interface Mapper<ID, R, EX extends Throwable> extends CheckedFunction1<Iterable<ID>, Publisher<Map<ID, R>>, EX> {

    static <ID, R, EX extends Throwable> Mapper<ID, R, EX> cached(Mapper<ID, R, EX> mapper) {
        return cached(mapper, ConcurrentHashMap::new);
    }

    static <ID, R, EX extends Throwable>
    Mapper<ID, R, EX> cached(Mapper<ID, R, EX> mapper, Supplier<Map<Iterable<ID>, Publisher<Map<ID, R>>>> cacheSupplier) {
        return cached(mapper, cacheSupplier, null);
    }

    static <ID, R, EX extends Throwable> Mapper<ID, R, EX> cached(Mapper<ID, R, EX> mapper, Duration ttl) {
        return cached(mapper, ConcurrentHashMap::new, ttl);
    }

    static <ID, R, EX extends Throwable>
    Mapper<ID, R, EX> cached(Mapper<ID, R, EX> mapper, Supplier<Map<Iterable<ID>, Publisher<Map<ID, R>>>> cacheSupplier, Duration ttl) {
        var cache = cacheSupplier.get();
        return entityIds -> cache.computeIfAbsent(entityIds, ids -> toCachedMono(from(mapper.apply(ids)), ttl));
    }

    private static <T> Mono<T> toCachedMono(Mono<T> mono, Duration ttl) {
        return ttl != null ? mono.cache(ttl) : mono.cache();
    }

    static <ID, R, EX extends Throwable> Mapper<ID, R, EX> oneToOne(
            CheckedFunction1<List<ID>, Publisher<R>, EX> queryFunction,
            Function<R, ID> idExtractorFromQueryResults) {

        return oneToOne(queryFunction, idExtractorFromQueryResults, id -> null, ArrayList::new, null);
    }

    static <ID, IDC extends Collection<ID>, R, EX extends Throwable> Mapper<ID, R, EX> oneToOne(
            CheckedFunction1<IDC, Publisher<R>, EX> queryFunction,
            Function<R, ID> idExtractorFromQueryResults,
            Supplier<IDC> idCollectionFactory) {

        return oneToOne(queryFunction, idExtractorFromQueryResults, id -> null, idCollectionFactory);
    }

    static <ID, R, EX extends Throwable> Mapper<ID, R, EX> oneToOne(
            CheckedFunction1<List<ID>, Publisher<R>, EX> queryFunction,
            Function<R, ID> idExtractorFromQueryResults,
            Function<ID, R> defaultResultProvider) {

        return oneToOne(queryFunction, idExtractorFromQueryResults, defaultResultProvider, ArrayList::new, null);
    }

    static <ID, R, EX extends Throwable> Mapper<ID, R, EX> oneToOne(
            CheckedFunction1<List<ID>, Publisher<R>, EX> queryFunction,
            Function<R, ID> idExtractorFromQueryResults,
            Function<ID, R> defaultResultProvider,
            MapFactory<ID, R> mapFactory) {

        return oneToOne(queryFunction, idExtractorFromQueryResults, defaultResultProvider, ArrayList::new, mapFactory);
    }

    static <ID, IDC extends Collection<ID>, R, EX extends Throwable> Mapper<ID, R, EX> oneToOne(
            CheckedFunction1<IDC, Publisher<R>, EX> queryFunction,
            Function<R, ID> idExtractorFromQueryResults,
            Function<ID, R> defaultResultProvider,
            Supplier<IDC> idCollectionFactory) {

        return oneToOne(queryFunction, idExtractorFromQueryResults, defaultResultProvider, idCollectionFactory, null);
    }

    @SuppressWarnings("unchecked")
    static <ID, IDC extends Collection<ID>, R, EX extends Throwable> Mapper<ID, R, EX> oneToOne(
            CheckedFunction1<IDC, Publisher<R>, EX> queryFunction,
            Function<R, ID> idExtractorFromQueryResults,
            Function<ID, R> defaultResultProvider,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, R> mapFactory) {

        return convertIdTypeMapperDelegate(entityIds ->
                queryOneToOne((IDC) entityIds, queryFunction, idExtractorFromQueryResults, defaultResultProvider, mapFactory), idCollectionFactory);
    }

    static <ID, R, EX extends Throwable> Mapper<ID, List<R>, EX> oneToManyAsList(
            CheckedFunction1<List<ID>, Publisher<R>, EX> queryFunction,
            Function<R, ID> idExtractorFromQueryResults) {

        return oneToManyAsList(queryFunction, idExtractorFromQueryResults, ArrayList::new, null);
    }

    static <ID, R, EX extends Throwable> Mapper<ID, List<R>, EX> oneToManyAsList(
            CheckedFunction1<List<ID>, Publisher<R>, EX> queryFunction,
            Function<R, ID> idExtractorFromQueryResults,
            MapFactory<ID, List<R>> mapFactory) {

        return oneToManyAsList(queryFunction, idExtractorFromQueryResults, ArrayList::new, mapFactory);
    }

    static <ID, IDC extends Collection<ID>, R, EX extends Throwable> Mapper<ID, List<R>, EX> oneToManyAsList(
            CheckedFunction1<IDC, Publisher<R>, EX> queryFunction,
            Function<R, ID> idExtractorFromQueryResults,
            Supplier<IDC> idCollectionFactory) {

        return oneToMany(queryFunction, idExtractorFromQueryResults, ArrayList::new, idCollectionFactory);
    }

    static <ID, IDC extends Collection<ID>, R, EX extends Throwable> Mapper<ID, List<R>, EX> oneToManyAsList(
            CheckedFunction1<IDC, Publisher<R>, EX> queryFunction,
            Function<R, ID> idExtractorFromQueryResults,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, List<R>> mapFactory) {

        return oneToMany(queryFunction, idExtractorFromQueryResults, ArrayList::new, idCollectionFactory, mapFactory);
    }

    static <ID, R, EX extends Throwable> Mapper<ID, Set<R>, EX> oneToManyAsSet(
            CheckedFunction1<Set<ID>, Publisher<R>, EX> queryFunction,
            Function<R, ID> idExtractorFromQueryResults) {

        return oneToManyAsSet(queryFunction, idExtractorFromQueryResults, HashSet::new, null);
    }

    static <ID, R, EX extends Throwable> Mapper<ID, Set<R>, EX> oneToManyAsSet(
            CheckedFunction1<Set<ID>, Publisher<R>, EX> queryFunction,
            Function<R, ID> idExtractorFromQueryResults,
            MapFactory<ID, Set<R>> mapFactory) {

        return oneToManyAsSet(queryFunction, idExtractorFromQueryResults, HashSet::new, mapFactory);
    }

    static <ID, IDC extends Collection<ID>, R, EX extends Throwable> Mapper<ID, Set<R>, EX> oneToManyAsSet(
            CheckedFunction1<IDC, Publisher<R>, EX> queryFunction,
            Function<R, ID> idExtractorFromQueryResults,
            Supplier<IDC> idCollectionFactory) {

        return oneToMany(queryFunction, idExtractorFromQueryResults, HashSet::new, idCollectionFactory);
    }

    static <ID, IDC extends Collection<ID>, R, EX extends Throwable> Mapper<ID, Set<R>, EX> oneToManyAsSet(
            CheckedFunction1<IDC, Publisher<R>, EX> queryFunction,
            Function<R, ID> idExtractorFromQueryResults,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, Set<R>> mapFactory) {

        return oneToMany(queryFunction, idExtractorFromQueryResults, HashSet::new, idCollectionFactory, mapFactory);
    }

    static <ID, IDC extends Collection<ID>, R, RC extends Collection<R>, EX extends Throwable> Mapper<ID, RC, EX> oneToMany(
            CheckedFunction1<IDC, Publisher<R>, EX> queryFunction,
            Function<R, ID> idExtractorFromQueryResults,
            Supplier<RC> collectionFactory,
            Supplier<IDC> idCollectionFactory) {

        return oneToMany(queryFunction, idExtractorFromQueryResults, collectionFactory, idCollectionFactory, null);
    }

    @SuppressWarnings("unchecked")
    static <ID, IDC extends Collection<ID>, R, RC extends Collection<R>, EX extends Throwable> Mapper<ID, RC, EX> oneToMany(
            CheckedFunction1<IDC, Publisher<R>, EX> queryFunction,
            Function<R, ID> idExtractorFromQueryResults,
            Supplier<RC> collectionFactory,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, RC> mapFactory) {

        return convertIdTypeMapperDelegate(entityIds ->
                queryOneToMany((IDC) entityIds, queryFunction, idExtractorFromQueryResults, collectionFactory, mapFactory), idCollectionFactory);
    }

    private static <ID, IDC extends Collection<ID>, R, EX extends Throwable> Mapper<ID, R, EX> convertIdTypeMapperDelegate(
            Mapper<ID, R, EX> mapper, Supplier<IDC> idCollectionFactory) {

        return entityIds -> mapper.apply(refineEntityIDType(entityIds, idCollectionFactory));
    }

    private static <ID, IDC extends Collection<ID>> IDC refineEntityIDType(Iterable<ID> entityIds, Supplier<IDC> idCollectionFactory) {

        return stream(entityIds.spliterator(), false)
                .collect(toCollection(idCollectionFactory));
    }
}
