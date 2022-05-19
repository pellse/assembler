package io.github.pellse.reactive.assembler;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static io.github.pellse.reactive.assembler.QueryUtils.*;
import static io.github.pellse.reactive.assembler.RuleMapperContext.toRuleMapperContext;
import static io.github.pellse.reactive.assembler.RuleMapperSource.call;
import static io.github.pellse.util.ObjectUtils.then;
import static io.github.pellse.util.collection.CollectionUtil.translate;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.*;

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
                ctx -> ids -> toMap(ctx.idExtractor(), identity(), (u1, u2) -> u2, toSupplier(ids, ctx.mapFactory())));
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
                ctx -> ids -> groupingBy(ctx.idExtractor(), toSupplier(ids, ctx.mapFactory()), toCollection(collectionFactory)));
    }

    static <ID, IDC extends Collection<ID>, R, RRC> RuleMapper<ID, IDC, R, RRC> createRuleMapper(
            RuleMapperSource<ID, IDC, R, RRC> ruleMapperSource,
            Function<ID, RRC> defaultResultProvider,
            Function<RuleContext<ID, IDC, R, RRC>, Function<IDC, Collector<R, ?, Map<ID, RRC>>>> mapCollector) {

        return ruleContext -> {
            var ruleMapperContext = toRuleMapperContext(
                    ruleContext,
                    defaultResultProvider,
                    mapCollector.apply(ruleContext));

            var queryFunction = ruleMapperSource.apply(ruleMapperContext);

            return entityIds ->
                    then(translate(entityIds, ruleMapperContext.idCollectionFactory()), ids ->
                            safeApply(ids, queryFunction)
                                    .collect(ruleMapperContext.mapCollector().apply(ids))
                                    .map(map -> toResultMap(ids, map, ruleMapperContext.defaultResultProvider())));
        };
    }
}
