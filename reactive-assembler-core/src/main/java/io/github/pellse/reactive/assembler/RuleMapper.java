package io.github.pellse.reactive.assembler;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.github.pellse.reactive.assembler.QueryUtils.queryOneToMany;
import static io.github.pellse.reactive.assembler.QueryUtils.queryOneToOne;
import static io.github.pellse.reactive.assembler.RuleMapperSource.call;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.StreamSupport.stream;

/**
 *
 * @param <ID> Correlation Id type
 * @param <IDC> Collection of correlation ids type (e.g. List<ID>, Set<ID>)
 * @param <R> Type of the publisher elements returned from <code>queryFunction</code>
 * @param <RRC> Either R or collection of R (e.g. R vs. List<R>)
 */
@FunctionalInterface
public interface RuleMapper<ID, IDC extends Collection<ID>, R, RRC> extends BiFunction<RuleContext<ID, IDC, R, RRC>, Iterable<ID>, Mono<Map<ID, RRC>>> {

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, R> oneToOne(Function<IDC, Publisher<R>> queryFunction) {
        return oneToOne(call(queryFunction), id -> null);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, R> oneToOne(RuleMapperSource<ID, IDC, R, R> ruleMapperSource) {
        return oneToOne(ruleMapperSource, id -> null);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, R> oneToOne(
            Function<IDC, Publisher<R>> queryFunction,
            Function<ID, R> defaultResultProvider) {
        return oneToOne(call(queryFunction), defaultResultProvider);
    }

    @SuppressWarnings("unchecked")
    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, R> oneToOne(
            RuleMapperSource<ID, IDC, R, R> ruleMapperSource,
            Function<ID, R> defaultResultProvider) {
        return convertIdTypeMapperDelegateOneToOne((ruleContext, entityIds) ->
                        queryOneToOne((IDC) entityIds,
                                ruleMapperSource.apply(ruleContext),
                                ruleContext.idExtractor(),
                                defaultResultProvider,
                                ruleContext.mapFactory()));
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

    @SuppressWarnings("unchecked")
    static <ID, IDC extends Collection<ID>, R, RC extends Collection<R>> RuleMapper<ID, IDC, R, RC> oneToMany(
            RuleMapperSource<ID, IDC, R, RC> ruleMapperSource,
            Supplier<RC> collectionFactory) {
        return convertIdTypeMapperDelegateOneToMany((ruleContext, entityIds) ->
                queryOneToMany((IDC) entityIds,
                        ruleMapperSource.apply(ruleContext),
                        ruleContext.idExtractor(),
                        collectionFactory,
                        ruleContext.mapFactory()));
    }

    private static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, R> convertIdTypeMapperDelegateOneToOne(
            RuleMapper<ID, IDC, R, R> mapper) {
        return (ruleContext, entityIds) -> mapper.apply(ruleContext, refineEntityIDType(entityIds, ruleContext.idCollectionFactory()));
    }

    private static <ID, IDC extends Collection<ID>, R, RC extends Collection<R>> RuleMapper<ID, IDC, R, RC> convertIdTypeMapperDelegateOneToMany(
            RuleMapper<ID, IDC, R, RC> mapper) {
        return (ruleContext, entityIds) -> mapper.apply(ruleContext, refineEntityIDType(entityIds, ruleContext.idCollectionFactory()));
    }

    private static <ID, IDC extends Collection<ID>> IDC refineEntityIDType(Iterable<ID> entityIds, Supplier<IDC> idCollectionFactory) {
        return stream(entityIds.spliterator(), false)
                .collect(toCollection(idCollectionFactory));
    }
}
