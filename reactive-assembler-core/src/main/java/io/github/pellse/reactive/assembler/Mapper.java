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

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
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
public interface Mapper<ID, R> extends Function<Iterable<@NotNull ID>, Publisher<Map<@NotNull ID, @NotNull R>>> {

    @NotNull
    static <ID, R> Mapper<@NotNull ID, @NotNull R> cached(Mapper<ID, R> mapper) {
        return cached(mapper, ConcurrentHashMap::new);
    }

    @NotNull
    static <ID, R> Mapper<@NotNull ID, @NotNull R> cached(Mapper<ID, R> mapper, Supplier<Map<Iterable<ID>, Publisher<Map<ID, R>>>> cacheSupplier) {
        return cached(mapper, cacheSupplier, null);
    }

    @NotNull
    static <ID, R> Mapper<@NotNull ID, @NotNull R> cached(Mapper<ID, R> mapper, Duration ttl) {
        return cached(mapper, ConcurrentHashMap::new, ttl);
    }

    @NotNull
    static <ID, R> Mapper<@NotNull ID, @NotNull R> cached(Mapper<ID, R> mapper, Supplier<Map<Iterable<ID>, Publisher<Map<ID, R>>>> cacheSupplier, Duration ttl) {
        var cache = cacheSupplier.get();
        return entityIds -> cache.computeIfAbsent(entityIds, ids -> toCachedMono(from(mapper.apply(ids)), ttl));
    }

    @NotNull
    private static <T> Mono<T> toCachedMono(Mono<T> mono, Duration ttl) {
        return ttl != null ? mono.cache(ttl) : mono.cache();
    }

    @NotNull
    @Contract(pure = true)
    static <ID, R> Mapper<@NotNull ID, @NotNull R> oneToOne(
            Function<List<ID>, Publisher<R>> queryFunction,
            Function<R, ID> idExtractorFromQueryResults) {

        return oneToOne(queryFunction, idExtractorFromQueryResults, id -> null, ArrayList::new, null);
    }

    @NotNull
    @Contract(pure = true)
    static <ID, IDC extends Collection<ID>, R> Mapper<@NotNull ID, @NotNull R> oneToOne(
            Function<IDC, Publisher<R>> queryFunction,
            Function<R, ID> idExtractorFromQueryResults,
            Supplier<IDC> idCollectionFactory) {

        return oneToOne(queryFunction, idExtractorFromQueryResults, id -> null, idCollectionFactory);
    }

    @NotNull
    @Contract(pure = true)
    static <ID, R> Mapper<@NotNull ID, @NotNull R> oneToOne(
            Function<List<ID>, Publisher<R>> queryFunction,
            Function<R, ID> idExtractorFromQueryResults,
            Function<ID, R> defaultResultProvider) {

        return oneToOne(queryFunction, idExtractorFromQueryResults, defaultResultProvider, ArrayList::new, null);
    }

    @NotNull
    @Contract(pure = true)
    static <ID, R> Mapper<@NotNull ID, @NotNull R> oneToOne(
            Function<List<ID>, Publisher<R>> queryFunction,
            Function<R, ID> idExtractorFromQueryResults,
            Function<ID, R> defaultResultProvider,
            MapFactory<ID, R> mapFactory) {

        return oneToOne(queryFunction, idExtractorFromQueryResults, defaultResultProvider, ArrayList::new, mapFactory);
    }

    @NotNull
    @Contract(pure = true)
    static <ID, IDC extends Collection<ID>, R> Mapper<@NotNull ID, @NotNull R> oneToOne(
            Function<IDC, Publisher<R>> queryFunction,
            Function<R, ID> idExtractorFromQueryResults,
            Function<ID, R> defaultResultProvider,
            Supplier<IDC> idCollectionFactory) {

        return oneToOne(queryFunction, idExtractorFromQueryResults, defaultResultProvider, idCollectionFactory, null);
    }

    @NotNull
    @Contract(pure = true)
    @SuppressWarnings("unchecked")
    static <ID, IDC extends Collection<ID>, R> Mapper<@NotNull ID, @NotNull R> oneToOne(
            Function<IDC, Publisher<R>> queryFunction,
            Function<R, ID> idExtractorFromQueryResults,
            Function<ID, R> defaultResultProvider,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, R> mapFactory) {

        return convertIdTypeMapperDelegate(entityIds ->
                queryOneToOne((IDC) entityIds, queryFunction, idExtractorFromQueryResults, defaultResultProvider, mapFactory), idCollectionFactory);
    }

    @NotNull
    @Contract(pure = true)
    static <ID, R> Mapper<@NotNull ID, @NotNull List<@NotNull R>> oneToMany(
            Function<List<ID>, Publisher<R>> queryFunction,
            Function<R, ID> idExtractorFromQueryResults) {

        return oneToMany(queryFunction, idExtractorFromQueryResults, ArrayList::new, (MapFactory<ID, List<R>>) null);
    }

    @NotNull
    @Contract(pure = true)
    static <ID, R> Mapper<@NotNull ID, @NotNull List<@NotNull R>> oneToMany(
            Function<List<ID>, Publisher<R>> queryFunction,
            Function<R, ID> idExtractorFromQueryResults,
            MapFactory<ID, List<R>> mapFactory) {

        return oneToMany(queryFunction, idExtractorFromQueryResults, ArrayList::new, mapFactory);
    }

    @NotNull
    @Contract(pure = true)
    static <ID, IDC extends Collection<ID>, R> Mapper<@NotNull ID, @NotNull List<@NotNull R>> oneToMany(
            Function<IDC, Publisher<R>> queryFunction,
            Function<R, ID> idExtractorFromQueryResults,
            Supplier<IDC> idCollectionFactory) {

        return oneToMany(queryFunction, idExtractorFromQueryResults, ArrayList::new, idCollectionFactory);
    }

    @NotNull
    @Contract(pure = true)
    static <ID, IDC extends Collection<ID>, R> Mapper<@NotNull ID, @NotNull List<@NotNull R>> oneToMany(
            Function<IDC, Publisher<R>> queryFunction,
            Function<R, ID> idExtractorFromQueryResults,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, List<R>> mapFactory) {

        return oneToMany(queryFunction, idExtractorFromQueryResults, ArrayList::new, idCollectionFactory, mapFactory);
    }

    @NotNull
    @Contract(pure = true)
    static <ID, R> Mapper<@NotNull ID, @NotNull Set<@NotNull R>> oneToManyAsSet(
            Function<Set<ID>, Publisher<R>> queryFunction,
            Function<R, ID> idExtractorFromQueryResults) {

        return oneToManyAsSet(queryFunction, idExtractorFromQueryResults, HashSet::new, null);
    }

    @NotNull
    @Contract(pure = true)
    static <ID, R> Mapper<@NotNull ID, @NotNull Set<@NotNull R>> oneToManyAsSet(
            Function<Set<ID>, Publisher<R>> queryFunction,
            Function<R, ID> idExtractorFromQueryResults,
            MapFactory<ID, Set<R>> mapFactory) {

        return oneToManyAsSet(queryFunction, idExtractorFromQueryResults, HashSet::new, mapFactory);
    }

    @NotNull
    @Contract(pure = true)
    static <ID, IDC extends Collection<ID>, R> Mapper<@NotNull ID, @NotNull Set<@NotNull R>> oneToManyAsSet(
            Function<IDC, Publisher<R>> queryFunction,
            Function<R, ID> idExtractorFromQueryResults,
            Supplier<IDC> idCollectionFactory) {

        return oneToMany(queryFunction, idExtractorFromQueryResults, HashSet::new, idCollectionFactory);
    }

    @NotNull
    @Contract(pure = true)
    static <ID, IDC extends Collection<ID>, R> Mapper<@NotNull ID, @NotNull Set<@NotNull R>> oneToManyAsSet(
            Function<IDC, Publisher<R>> queryFunction,
            Function<R, ID> idExtractorFromQueryResults,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, Set<R>> mapFactory) {

        return oneToMany(queryFunction, idExtractorFromQueryResults, HashSet::new, idCollectionFactory, mapFactory);
    }

    @NotNull
    @Contract(pure = true)
    static <ID, IDC extends Collection<ID>, R, RC extends Collection<@NotNull R>> Mapper<@NotNull ID, @NotNull RC> oneToMany(
            Function<IDC, Publisher<R>> queryFunction,
            Function<R, ID> idExtractorFromQueryResults,
            Supplier<RC> collectionFactory,
            Supplier<IDC> idCollectionFactory) {

        return oneToMany(queryFunction, idExtractorFromQueryResults, collectionFactory, idCollectionFactory, null);
    }

    @NotNull
    @Contract(pure = true)
    @SuppressWarnings("unchecked")
    static <ID, IDC extends Collection<ID>, R, RC extends Collection<@NotNull R>> Mapper<@NotNull ID, @NotNull RC> oneToMany(
            Function<IDC, Publisher<R>> queryFunction,
            Function<R, ID> idExtractorFromQueryResults,
            Supplier<RC> collectionFactory,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, RC> mapFactory) {

        return convertIdTypeMapperDelegate(entityIds ->
                queryOneToMany((IDC) entityIds, queryFunction, idExtractorFromQueryResults, collectionFactory, mapFactory), idCollectionFactory);
    }

    @NotNull
    @Contract(pure = true)
    private static <ID, IDC extends Collection<ID>, R> Mapper<@NotNull ID, @NotNull R> convertIdTypeMapperDelegate(
            Mapper<ID, R> mapper, Supplier<IDC> idCollectionFactory) {

        return entityIds -> mapper.apply(refineEntityIDType(entityIds, idCollectionFactory));
    }

    private static <ID, IDC extends Collection<ID>> IDC refineEntityIDType(Iterable<ID> entityIds, Supplier<IDC> idCollectionFactory) {

        return stream(entityIds.spliterator(), false)
                .collect(toCollection(idCollectionFactory));
    }
}
