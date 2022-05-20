package io.github.pellse.reactive.assembler;

import io.github.pellse.reactive.assembler.caching.MergeStrategy;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static io.github.pellse.reactive.assembler.QueryUtils.*;
import static io.github.pellse.reactive.assembler.RuleMapperContext.toRuleMapperContext;
import static io.github.pellse.reactive.assembler.RuleMapperSource.call;
import static io.github.pellse.util.ObjectUtils.also;
import static io.github.pellse.util.ObjectUtils.then;
import static io.github.pellse.util.collection.CollectionUtil.*;
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
                        toMap(ctx.idExtractor(), identity(), (u1, u2) -> u2, toSupplier(validate(initialMapCapacity), ctx.mapFactory())),
                identity(),
                replaceStrategy());
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, List<R>> oneToMany(
            Function<IDC, Publisher<R>> queryFunction) {
        return oneToMany(call(queryFunction), ArrayList::new);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, List<R>> oneToMany(
            RuleMapperSource<ID, IDC, R, List<R>> ruleMapperSource) {
        return oneToMany(ruleMapperSource, ArrayList::new);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, Set<R>> oneToManyAsSet(
            Function<IDC, Publisher<R>> queryFunction) {
        return oneToMany(call(queryFunction), HashSet::new);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, Set<R>> oneToManyAsSet(
            RuleMapperSource<ID, IDC, R, Set<R>> ruleMapperSource) {
        return oneToMany(ruleMapperSource, HashSet::new);
    }

    static <ID, IDC extends Collection<ID>, R, RC extends Collection<R>> RuleMapper<ID, IDC, R, RC> oneToMany(
            Function<IDC, Publisher<R>> queryFunction,
            Supplier<RC> collectionFactory) {
        return oneToMany(call(queryFunction), collectionFactory);
    }

    static <ID, IDC extends Collection<ID>, R, RC extends Collection<R>> RuleMapper<ID, IDC, R, RC> oneToMany(
            RuleMapperSource<ID, IDC, R, RC> ruleMapperSource,
            Supplier<RC> collectionFactory) {

        return createRuleMapper(
                ruleMapperSource,
                id -> collectionFactory.get(),
                ctx -> initialMapCapacity ->
                        groupingBy(ctx.idExtractor(), toSupplier(validate(initialMapCapacity), ctx.mapFactory()), toCollection(collectionFactory)),
                stream -> stream.flatMap(Collection::stream),
                appendValuesStrategy(collectionFactory));
    }

    private static <ID, IDC extends Collection<ID>, R, RRC> RuleMapper<ID, IDC, R, RRC> createRuleMapper(
            RuleMapperSource<ID, IDC, R, RRC> ruleMapperSource,
            Function<ID, RRC> defaultResultProvider,
            Function<RuleContext<ID, IDC, R, RRC>, Function<Integer, Collector<R, ?, Map<ID, RRC>>>> mapCollector,
            Function<Stream<RRC>, Stream<R>> streamFlattener,
            MergeStrategy<ID, RRC> mergeStrategy) {

        return ruleContext -> {
            var ruleMapperContext = toRuleMapperContext(
                    ruleContext,
                    defaultResultProvider,
                    mapCollector.apply(ruleContext),
                    streamFlattener,
                    mergeStrategy);

            var queryFunction = ruleMapperSource.apply(ruleMapperContext);

            return entityIds ->
                    then(translate(entityIds, ruleMapperContext.idCollectionFactory()), ids ->
                            safeApply(ids, queryFunction)
                                    .collect(ruleMapperContext.mapCollector().apply(ids.size()))
                                    .map(map -> toResultMap(ids, map, ruleMapperContext.defaultResultProvider())));
        };
    }

    private static <ID, R> MergeStrategy<ID, R> replaceStrategy() {
        return (cache, map) -> also(cache, m -> m.putAll(map));
    }

    private static <ID, R, RC extends Collection<R>> MergeStrategy<ID, RC> appendValuesStrategy(Supplier<RC> collectionFactory) {

        return (cache, map) -> {
            cache.replaceAll((id, coll) ->
                    concat(toStream(coll), toStream(map.get(id)))
                            .distinct()
                            .collect(toCollection(collectionFactory)));

            return mergeMaps(cache, readAll(intersect(map.keySet(), cache.keySet()), map));
        };
    }

    private static int validate(int initialCapacity) {
        return Math.max(initialCapacity, 0);
    }
}
