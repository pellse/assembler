package io.github.pellse.reactive.assembler;

import io.github.pellse.reactive.assembler.caching.MergeStrategy;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static io.github.pellse.reactive.assembler.QueryUtils.*;
import static io.github.pellse.reactive.assembler.RuleMapperContext.toRuleMapperContext;
import static io.github.pellse.reactive.assembler.RuleMapperSource.call;
import static io.github.pellse.util.ObjectUtils.also;
import static io.github.pellse.util.ObjectUtils.then;
import static io.github.pellse.util.collection.CollectionUtil.*;
import static java.util.Collections.unmodifiableMap;
import static java.util.Map.entry;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.*;
import static java.util.stream.Stream.concat;

/**
 * @param <ID>  Correlation Id type
 * @param <IDC> Collection of correlation ids type (e.g. {@code List<ID>}, {@code Set<ID>})
 * @param <R>   Type of the publisher elements returned from {@code queryFunction}
 * @param <RRC> Either R or collection of R (e.g. R vs. {@code List<R>})
 */
@FunctionalInterface
public interface RuleMapper<ID, IDC extends Collection<ID>, R, RRC>
        extends Function<RuleContext<ID, IDC, R, RRC>, Function<Iterable<ID>, Mono<Map<ID, RRC>>>> {

    record Wrapper<ID, EID, R>(ID correlationId, EID id, R payload) {
        @Override
        public boolean equals(Object obj) {
            return Objects.equals(id, obj);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(id);
        }
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, R> oneToOne(
            Function<IDC, Publisher<R>> queryFunction) {
        return oneToOne(call(queryFunction), id -> null);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, R> oneToOne(
            RuleMapperSource<ID, IDC, R, R> ruleMapperSource) {
        return oneToOne(ruleMapperSource, id -> null);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, R> oneToOne(
            Function<IDC, Publisher<R>> queryFunction,
            Function<ID, R> defaultResultProvider) {
        return oneToOne(call(queryFunction), defaultResultProvider);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, R> oneToOne(
            RuleMapperSource<ID, IDC, R, R> ruleMapperSource,
            Function<ID, R> defaultResultProvider) {

        return createRuleMapper(
                ruleMapperSource,
                defaultResultProvider,
                ctx -> initialMapCapacity ->
                        toMap(ctx.correlationIdExtractor(), identity(), (u1, u2) -> u2, toSupplier(validate(initialMapCapacity), ctx.mapFactory())),
                identity(),
                updateStrategy(),
                removeStrategy());
    }

//    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, List<R>> oneToMany(
//            Function<IDC, Publisher<R>> queryFunction) {
//        return ruleContext -> oneToMany(ruleContext.correlationIdExtractor(), queryFunction).apply(ruleContext);
//    }
//
//    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, List<R>> oneToMany(
//            RuleMapperSource<ID, IDC, R, List<R>> ruleMapperSource) {
//        return ruleContext -> oneToMany(ruleContext.correlationIdExtractor(), ruleMapperSource).apply(ruleContext);
//    }
//
//    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, Set<R>> oneToManyAsSet(
//            Function<IDC, Publisher<R>> queryFunction) {
//        return ruleContext -> oneToManyAsSet(ruleContext.correlationIdExtractor(), queryFunction).apply(ruleContext);
//    }
//
//    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, Set<R>> oneToManyAsSet(
//            RuleMapperSource<ID, IDC, R, Set<R>> ruleMapperSource) {
//        return ruleContext -> oneToManyAsSet(ruleContext.correlationIdExtractor(), ruleMapperSource).apply(ruleContext);
//    }
//
//    static <ID, IDC extends Collection<ID>, R, RC extends Collection<R>> RuleMapper<ID, IDC, R, RC> oneToMany(
//            Function<IDC, Publisher<R>> queryFunction,
//            Supplier<RC> collectionFactory) {
//        return ruleContext -> oneToMany(ruleContext.correlationIdExtractor(), queryFunction, collectionFactory).apply(ruleContext);
//    }
//
//    static <ID, IDC extends Collection<ID>, R, RC extends Collection<R>> RuleMapper<ID, IDC, R, RC> oneToMany(
//            RuleMapperSource<ID, IDC, R, RC> ruleMapperSource,
//            Supplier<RC> collectionFactory) {
//        return ruleContext -> oneToMany(ruleContext.correlationIdExtractor(), ruleMapperSource, collectionFactory).apply(ruleContext);
//    }

    static <ID, EID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, List<R>> oneToMany(
            Function<R, EID> idExtractor,
            Function<IDC, Publisher<R>> queryFunction) {
        return oneToMany(idExtractor, call(queryFunction), ArrayList::new);
    }

    static <ID, EID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, List<R>> oneToMany(
            Function<R, EID> idExtractor,
            RuleMapperSource<ID, IDC, R, List<R>> ruleMapperSource) {
        return oneToMany(idExtractor, ruleMapperSource, ArrayList::new);
    }

    static <ID, EID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, Set<R>> oneToManyAsSet(
            Function<R, EID> idExtractor,
            Function<IDC, Publisher<R>> queryFunction) {
        return oneToMany(idExtractor, call(queryFunction), HashSet::new);
    }

    static <ID, EID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, Set<R>> oneToManyAsSet(
            Function<R, EID> idExtractor,
            RuleMapperSource<ID, IDC, R, Set<R>> ruleMapperSource) {
        return oneToMany(idExtractor, ruleMapperSource, HashSet::new);
    }

    static <ID, EID, IDC extends Collection<ID>, R, RC extends Collection<R>> RuleMapper<ID, IDC, R, RC> oneToMany(
            Function<R, EID> idExtractor,
            Function<IDC, Publisher<R>> queryFunction,
            Supplier<RC> collectionFactory) {
        return oneToMany(idExtractor, call(queryFunction), collectionFactory);
    }

    static <ID, EID, IDC extends Collection<ID>, R, RC extends Collection<R>> RuleMapper<ID, IDC, R, RC> oneToMany(
            Function<R, EID> idExtractor,
            RuleMapperSource<ID, IDC, R, RC> ruleMapperSource,
            Supplier<RC> collectionFactory) {

        return createRuleMapper(
                ruleMapperSource,
                id -> collectionFactory.get(),
                ctx -> initialMapCapacity ->
                        groupingBy(ctx.correlationIdExtractor(), toSupplier(validate(initialMapCapacity), ctx.mapFactory()), toCollection(collectionFactory)),
                stream -> stream.flatMap(Collection::stream),
                updateMultiStrategy(idExtractor, collectionFactory),
                removeMultiStrategy(idExtractor, collectionFactory));
    }

    private static <ID, IDC extends Collection<ID>, R, RRC> RuleMapper<ID, IDC, R, RRC> createRuleMapper(
            RuleMapperSource<ID, IDC, R, RRC> ruleMapperSource,
            Function<ID, RRC> defaultResultProvider,
            Function<RuleContext<ID, IDC, R, RRC>, IntFunction<Collector<R, ?, Map<ID, RRC>>>> mapCollector,
            Function<Stream<RRC>, Stream<R>> streamFlattener,
            MergeStrategy<ID, RRC> mergeStrategy,
            MergeStrategy<ID, RRC> removeStrategy) {

        return ruleContext -> {
            var ruleMapperContext = toRuleMapperContext(
                    ruleContext,
                    defaultResultProvider,
                    mapCollector.apply(ruleContext),
                    streamFlattener,
                    safeStrategy(mergeStrategy),
                    safeStrategy(removeStrategy));

            var queryFunction = ruleMapperSource.apply(ruleMapperContext);

            return entityIds ->
                    then(translate(entityIds, ruleMapperContext.idCollectionFactory()), ids ->
                            safeApply(ids, queryFunction)
                                    .collect(ruleMapperContext.mapCollector().apply(ids.size()))
                                    .map(map -> toResultMap(ids, map, ruleMapperContext.defaultResultProvider())));
        };
    }

    private static <ID, R> MergeStrategy<ID, R> updateStrategy() {
        return (cache, itemsToUpdateMap) -> itemsToUpdateMap;
    }

    private static <ID, EID, R, RC extends Collection<R>> MergeStrategy<ID, RC> updateMultiStrategy(
            Function<R, EID> idExtractor,
            Supplier<RC> collectionFactory) {

        return (cacheQueryResults, itemsToUpdateMap) -> {

            var newItems = toStream(itemsToUpdateMap.entrySet());
            return concat(toStream(cacheQueryResults.entrySet()), newItems)
                    .flatMap(entry -> entry.getValue().stream()
                            .map(e -> new Wrapper<>(entry.getKey(), idExtractor.apply(e), e)))
                    .distinct()
                    .collect(groupingBy(Wrapper::correlationId, mapping(Wrapper::payload, toCollection(collectionFactory))));
        };
    }

    private static <ID, R> MergeStrategy<ID, R> removeStrategy() {
        return (cacheQueryResults, itemsToRemoveMap) -> also(cacheQueryResults, c -> c.keySet().removeAll(itemsToRemoveMap.keySet()));
    }

    private static <ID, EID, R, RC extends Collection<R>> MergeStrategy<ID, RC> removeMultiStrategy(
            Function<R, EID> idExtractor,
            Supplier<RC> collectionFactory) {

        return (cacheQueryResults, itemsToRemoveMap) -> cacheQueryResults.entrySet().stream()
                .map(entry -> {
                    var itemsToRemove = itemsToRemoveMap.get(entry.getKey());
                    if (itemsToRemove == null)
                        return entry;

                    var idsToRemove = itemsToRemove.stream()
                            .map(idExtractor)
                            .collect(toSet());

                    var newColl = toStream(entry.getValue())
                            .filter(element -> !idsToRemove.contains(idExtractor.apply(element)))
                            .collect(toCollection(collectionFactory));

                    return isNotEmpty(newColl) ? entry(entry.getKey(), newColl) : null;
                })
                .filter(Objects::nonNull)
                .collect(toMap(Entry::getKey, Entry::getValue, (v1, v2) -> v1));
    }

    private static <ID, RRC> MergeStrategy<ID, RRC> safeStrategy(MergeStrategy<ID, RRC> strategy) {
        return (cacheQueryResults, itemsToUpdateMap) -> strategy.merge(new HashMap<>(cacheQueryResults), unmodifiableMap(itemsToUpdateMap));
    }

    private static int validate(int initialCapacity) {
        return Math.max(initialCapacity, 0);
    }
}
