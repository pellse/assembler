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

package io.github.pellse.assembler;

import io.github.pellse.assembler.caching.MergeStrategy;
import io.github.pellse.util.collection.CollectionUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static io.github.pellse.assembler.QueryUtils.*;
import static io.github.pellse.assembler.RuleContext.IdAwareRuleContext.*;
import static io.github.pellse.assembler.RuleMapperContext.toRuleMapperContext;
import static io.github.pellse.assembler.RuleMapperSource.*;
import static io.github.pellse.util.ObjectUtils.then;
import static io.github.pellse.util.collection.CollectionUtils.*;
import static java.util.Map.entry;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.*;

/**
 * @param <ID>  Correlation Id type
 * @param <TC>  Collection of correlation ids type (e.g. {@code List<ID>}, {@code Set<ID>})
 * @param <R>   Type of the publisher elements returned from {@code queryFunction}
 * @param <RRC> Either R or collection of R (e.g. R vs. {@code List<R>})
 */
@FunctionalInterface
public interface RuleMapper<T, TC extends Collection<T>, ID, R, RRC>
        extends Function<RuleContext<T, TC, ID, R, RRC>, Function<Iterable<T>, Mono<Map<ID, RRC>>>> {

    static <T, TC extends Collection<T>, ID, R> RuleMapper<T, TC, ID, R, R> oneToOne() {
        return oneToOne(emptySource(), id -> null);
    }

    static <T, TC extends Collection<T>, ID, R> RuleMapper<T, TC, ID, R, R> oneToOne(Function<TC, Publisher<R>> queryFunction) {
        return oneToOne(toQueryFunction(queryFunction), id -> null);
    }

    static <T, TC extends Collection<T>, ID, R> RuleMapper<T, TC, ID, R, R> oneToOne(RuleMapperSource<T, TC, ID, ID, R, R> ruleMapperSource) {
        return oneToOne(ruleMapperSource, id -> null);
    }

    static <T, TC extends Collection<T>, ID, R> RuleMapper<T, TC, ID, R, R> oneToOne(
            Function<TC, Publisher<R>> queryFunction,
            Function<ID, R> defaultResultProvider) {

        return oneToOne(toQueryFunction(queryFunction), defaultResultProvider);
    }

    static <T, TC extends Collection<T>, ID, R> RuleMapper<T, TC, ID, R, R> oneToOne(
            RuleMapperSource<T, TC, ID, ID, R, R> ruleMapperSource,
            Function<ID, R> defaultResultProvider) {

        return createRuleMapper(
                ruleMapperSource,
                ctx -> toIdAwareRuleContext(ctx.correlationIdResolver(), ctx),
                defaultResultProvider,
                ctx -> initialMapCapacity ->
                        toMap(ctx.correlationIdResolver(), identity(), (u1, u2) -> u2, toSupplier(validate(initialMapCapacity), ctx.mapFactory())),
                CollectionUtils::first,
                Collections::singletonList);
    }

    static <T, TC extends Collection<T>, ID, EID, R> RuleMapper<T, TC, ID, R, List<R>> oneToMany(Function<R, EID> idResolver) {
        return oneToMany(idResolver, emptySource(), ArrayList::new);
    }

    static <T, TC extends Collection<T>, ID, EID, R> RuleMapper<T, TC, ID, R, List<R>> oneToMany(
            Function<R, EID> idResolver,
            Function<TC, Publisher<R>> queryFunction) {

        return oneToMany(idResolver, toQueryFunction(queryFunction), ArrayList::new);
    }

    static <T, TC extends Collection<T>, ID, EID, R> RuleMapper<T, TC, ID, R, List<R>> oneToMany(
            Function<R, EID> idResolver,
            RuleMapperSource<T, TC, ID, EID, R, List<R>> ruleMapperSource) {

        return oneToMany(idResolver, ruleMapperSource, ArrayList::new);
    }

    static <T, TC extends Collection<T>, ID, EID, R> RuleMapper<T, TC, ID, R, Set<R>> oneToManyAsSet(
            Function<R, EID> idResolver,
            Function<TC, Publisher<R>> queryFunction) {

        return oneToMany(idResolver, toQueryFunction(queryFunction), HashSet::new);
    }

    static <T, TC extends Collection<T>, ID, EID, R> RuleMapper<T, TC, ID, R, Set<R>> oneToManyAsSet(
            Function<R, EID> idResolver,
            RuleMapperSource<T, TC, ID, EID, R, Set<R>> ruleMapperSource) {

        return oneToMany(idResolver, ruleMapperSource, HashSet::new);
    }

    static <T, TC extends Collection<T>, ID, EID, R, RC extends Collection<R>> RuleMapper<T, TC, ID, R, RC> oneToMany(
            Function<R, EID> idResolver,
            Function<TC, Publisher<R>> queryFunction,
            Supplier<RC> collectionFactory) {

        return oneToMany(idResolver, toQueryFunction(queryFunction), collectionFactory);
    }

    static <T, TC extends Collection<T>, ID, EID, R, RC extends Collection<R>> RuleMapper<T, TC, ID, R, RC> oneToMany(
            Function<R, EID> idResolver,
            RuleMapperSource<T, TC, ID, EID, R, RC> ruleMapperSource,
            Supplier<RC> collectionFactory) {

        return createRuleMapper(
                ruleMapperSource,
                ctx -> toIdAwareRuleContext(idResolver, ctx),
                id -> collectionFactory.get(),
                ctx -> initialMapCapacity ->
                        groupingBy(
                                ctx.correlationIdResolver(),
                                toSupplier(validate(initialMapCapacity), ctx.mapFactory()),
                                toCollection(collectionFactory)),
                list -> toStream(list)
                        .collect(toCollection(collectionFactory)),
                List::copyOf);
    }

    private static <T, TC extends Collection<T>, ID, EID, R, RRC> RuleMapper<T, TC, ID, R, RRC> createRuleMapper(
            RuleMapperSource<T, TC, ID, EID, R, RRC> ruleMapperSource,
            Function<RuleContext<T, TC, ID, R, RRC>, IdAwareRuleContext<T, TC, ID, EID, R, RRC>> ruleContextConverter,
            Function<ID, RRC> defaultResultProvider,
            Function<RuleContext<T, TC, ID, R, RRC>, IntFunction<Collector<R, ?, Map<ID, RRC>>>> mapCollector,
            Function<List<R>, RRC> fromListConverter,
            Function<RRC, List<R>> toListConverter) {

        return ruleContext -> {
            final var ruleMapperContext = toRuleMapperContext(
                    ruleContextConverter.apply(ruleContext),
                    defaultResultProvider,
                    mapCollector.apply(ruleContext),
                    fromListConverter,
                    toListConverter);

            final var queryFunction = nullToEmptySource(ruleMapperSource).apply(ruleMapperContext);

            return entityList ->
                    then(translate(entityList, ruleMapperContext.topLevelCollectionFactory()), entities ->
                            safeApply(entities, queryFunction)
                                    .collect(ruleMapperContext.mapCollector().apply(entities.size()))
                                    .map(map -> toResultMap(entities, map, ruleMapperContext.topLevelIdResolver(), ruleMapperContext.defaultResultProvider())));
        };
    }

    //    private static <ID, R> MergeStrategy<ID, R> updateStrategy() {
//        return (cache, itemsToUpdateMap) -> itemsToUpdateMap;
//    }
//
//    private static <ID, EID, R, RC extends Collection<R>> MergeStrategy<ID, RC> updateMultiStrategy(
//            Function<R, EID> idResolver,
//            Supplier<RC> collectionFactory) {
//
//        return (existingCacheItems, itemsToUpdateMap) ->
//                concat(toStream(existingCacheItems.entrySet()), toStream(itemsToUpdateMap.entrySet()))
//                        .flatMap(entry -> entry.getValue().stream()
//                                .map(e -> new Wrapper<>(entry.getKey(), idResolver.apply(e), e)))
//                        .distinct()
//                        .collect(groupingBy(Wrapper::correlationId, mapping(Wrapper::payload, toCollection(collectionFactory))));
//    }
//
//    private static <ID, R> MergeStrategy<ID, R> removeStrategy() {
//        return (existingCacheItems, itemsToRemoveMap) -> also(existingCacheItems, c -> c.keySet().removeAll(itemsToRemoveMap.keySet()));
//    }
//
    private static <ID, EID, R> MergeStrategy<ID, R> removeMultiStrategy(Function<R, EID> idResolver) {

        return (existingCacheItems, itemsToRemoveMap) -> existingCacheItems.entrySet().stream()
                .map(entry -> {
                    final var itemsToRemove = itemsToRemoveMap.get(entry.getKey());
                    if (itemsToRemove == null)
                        return entry;

                    final var idsToRemove = itemsToRemove.stream()
                            .map(idResolver)
                            .collect(toSet());

                    final var newColl = toStream(entry.getValue())
                            .filter(element -> !idsToRemove.contains(idResolver.apply(element)))
                            .toList();

                    return isNotEmpty(newColl) ? entry(entry.getKey(), newColl) : null;
                })
                .filter(Objects::nonNull)
                .collect(toMap(Entry::getKey, Entry::getValue, (v1, v2) -> v1));
    }
//
//    private static <ID, R> MergeStrategy<ID, R> safeStrategy(MergeStrategy<ID, R> strategy) {
//        return (existingCacheItems, itemsToUpdateMap) -> strategy.merge(new HashMap<>(existingCacheItems), unmodifiableMap(itemsToUpdateMap));
//    }

    private static int validate(int initialCapacity) {
        return Math.max(initialCapacity, 0);
    }
}
