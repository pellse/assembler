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

@FunctionalInterface
public interface RuleMapper<ID, IDC extends Collection<ID>, T, R> extends BiFunction<RuleContext<ID, IDC, T>, Iterable<ID>, Mono<Map<ID, R>>> {

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, R> oneToOne(Function<IDC, Publisher<R>> queryFunction) {
        return oneToOne(call(queryFunction), id -> null);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, R> oneToOne(RuleMapperSource<ID, IDC, R> ruleMapperSource) {
        return oneToOne(ruleMapperSource, id -> null);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, R> oneToOne(
            Function<IDC, Publisher<R>> queryFunction,
            Function<ID, R> defaultResultProvider) {
        return oneToOne(call(queryFunction), defaultResultProvider, null);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, R> oneToOne(
            RuleMapperSource<ID, IDC, R> ruleMapperSource,
            Function<ID, R> defaultResultProvider) {
        return oneToOne(ruleMapperSource, defaultResultProvider, null);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, R> oneToOne(
            Function<IDC, Publisher<R>> queryFunction,
            Function<ID, R> defaultResultProvider,
            MapFactory<ID, R> mapFactory) {
        return oneToOne(call(queryFunction), defaultResultProvider, mapFactory);
    }

    @SuppressWarnings("unchecked")
    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, R> oneToOne(
            RuleMapperSource<ID, IDC, R> ruleMapperSource,
            Function<ID, R> defaultResultProvider,
            MapFactory<ID, R> mapFactory) {
        return convertIdTypeMapperDelegateOneToOne((ruleContext, entityIds) ->
                        queryOneToOne((IDC) entityIds,
                                ruleMapperSource.apply(ruleContext),
                                ruleContext.idExtractor(),
                                defaultResultProvider,
                                mapFactory));
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, List<R>> oneToMany(
            Function<IDC, Publisher<R>> queryFunction) {
        return oneToMany(call(queryFunction), ArrayList::new, null);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, List<R>> oneToMany(
            RuleMapperSource<ID, IDC, R> ruleMapperSource) {
        return oneToMany(ruleMapperSource, ArrayList::new, null);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, List<R>> oneToMany(
            Function<IDC, Publisher<R>> queryFunction,
            MapFactory<ID, List<R>> mapFactory) {
        return oneToMany(call(queryFunction), ArrayList::new, mapFactory);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, List<R>> oneToMany(
            RuleMapperSource<ID, IDC, R> ruleMapperSource,
            MapFactory<ID, List<R>> mapFactory) {
        return oneToMany(ruleMapperSource, ArrayList::new, mapFactory);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, Set<R>> oneToManyAsSet(
            Function<IDC, Publisher<R>> queryFunction) {
        return oneToMany(call(queryFunction), HashSet::new, null);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, Set<R>> oneToManyAsSet(
            RuleMapperSource<ID, IDC, R> ruleMapperSource) {
        return oneToMany(ruleMapperSource, HashSet::new, null);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, Set<R>> oneToManyAsSet(
            Function<IDC, Publisher<R>> queryFunction,
            MapFactory<ID, Set<R>> mapFactory) {
        return oneToMany(call(queryFunction), HashSet::new, mapFactory);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, IDC, R, Set<R>> oneToManyAsSet(
            RuleMapperSource<ID, IDC, R> ruleMapperSource,
            MapFactory<ID, Set<R>> mapFactory) {
        return oneToMany(ruleMapperSource, HashSet::new, mapFactory);
    }

    static <ID, IDC extends Collection<ID>, R, RC extends Collection<R>> RuleMapper<ID, IDC, R, RC> oneToMany(
            Function<IDC, Publisher<R>> queryFunction,
            Supplier<RC> collectionFactory) {
        return oneToMany(call(queryFunction), collectionFactory, null);
    }

    static <ID, IDC extends Collection<ID>, R, RC extends Collection<R>> RuleMapper<ID, IDC, R, RC> oneToMany(
            RuleMapperSource<ID, IDC, R> ruleMapperSource,
            Supplier<RC> collectionFactory) {
        return oneToMany(ruleMapperSource, collectionFactory, null);
    }

    static <ID, IDC extends Collection<ID>, R, RC extends Collection<R>> RuleMapper<ID, IDC, R, RC> oneToMany(
            Function<IDC, Publisher<R>> queryFunction,
            Supplier<RC> collectionFactory,
            MapFactory<ID, RC> mapFactory) {
        return oneToMany(call(queryFunction), collectionFactory, mapFactory);
    }

    @SuppressWarnings("unchecked")
    static <ID, IDC extends Collection<ID>, R, RC extends Collection<R>> RuleMapper<ID, IDC, R, RC> oneToMany(
            RuleMapperSource<ID, IDC, R> ruleMapperSource,
            Supplier<RC> collectionFactory,
            MapFactory<ID, RC> mapFactory) {
        return convertIdTypeMapperDelegateOneToMany((ruleContext, entityIds) ->
                queryOneToMany((IDC) entityIds,
                        ruleMapperSource.apply(ruleContext),
                        ruleContext.idExtractor(),
                        collectionFactory,
                        mapFactory));
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
