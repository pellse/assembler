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
public interface RuleMapper<ID, T, R> extends BiFunction<Function<T, ID>, Iterable<ID>, Mono<Map<ID, R>>> {

    static <ID, R> RuleMapper<ID, R, R> oneToOne(Function<List<ID>, Publisher<R>> queryFunction) {
        return oneToOne(call(queryFunction), id -> null, ArrayList::new, null);
    }

    static <ID, R> RuleMapper<ID, R, R> oneToOne(RuleMapperSource<ID, List<ID>, R> ruleMapperSource) {
        return oneToOne(ruleMapperSource, id -> null, ArrayList::new, null);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, R, R> oneToOne(
            Function<IDC, Publisher<R>> queryFunction,
            Supplier<IDC> idCollectionFactory) {
        return oneToOne(call(queryFunction), id -> null, idCollectionFactory);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, R, R> oneToOne(
            RuleMapperSource<ID, IDC, R> ruleMapperSource,
            Supplier<IDC> idCollectionFactory) {
        return oneToOne(ruleMapperSource, id -> null, idCollectionFactory);
    }

    static <ID, R> RuleMapper<ID, R, R> oneToOne(
            Function<List<ID>, Publisher<R>> queryFunction,
            Function<ID, R> defaultResultProvider) {
        return oneToOne(call(queryFunction), defaultResultProvider, ArrayList::new, null);
    }

    static <ID, R> RuleMapper<ID, R, R> oneToOne(
            RuleMapperSource<ID, List<ID>, R> ruleMapperSource,
            Function<ID, R> defaultResultProvider) {
        return oneToOne(ruleMapperSource, defaultResultProvider, ArrayList::new, null);
    }

    static <ID, R> RuleMapper<ID, R, R> oneToOne(
            Function<List<ID>, Publisher<R>> queryFunction,
            Function<ID, R> defaultResultProvider,
            MapFactory<ID, R> mapFactory) {
        return oneToOne(call(queryFunction), defaultResultProvider, ArrayList::new, mapFactory);
    }

    static <ID, R> RuleMapper<ID, R, R> oneToOne(
            RuleMapperSource<ID, List<ID>, R> ruleMapperSource,
            Function<ID, R> defaultResultProvider,
            MapFactory<ID, R> mapFactory) {
        return oneToOne(ruleMapperSource, defaultResultProvider, ArrayList::new, mapFactory);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, R, R> oneToOne(
            Function<IDC, Publisher<R>> queryFunction,
            Function<ID, R> defaultResultProvider,
            Supplier<IDC> idCollectionFactory) {
        return oneToOne(call(queryFunction), defaultResultProvider, idCollectionFactory, null);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, R, R> oneToOne(
            RuleMapperSource<ID, IDC, R> ruleMapperSource,
            Function<ID, R> defaultResultProvider,
            Supplier<IDC> idCollectionFactory) {
        return oneToOne(ruleMapperSource, defaultResultProvider, idCollectionFactory, null);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, R, R> oneToOne(
            Function<IDC, Publisher<R>> queryFunction,
            Function<ID, R> defaultResultProvider,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, R> mapFactory) {
        return oneToOne(call(queryFunction), defaultResultProvider, idCollectionFactory, mapFactory);
    }

    @SuppressWarnings("unchecked")
    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, R, R> oneToOne(
            RuleMapperSource<ID, IDC, R> ruleMapperSource,
            Function<ID, R> defaultResultProvider,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, R> mapFactory) {
        return convertIdTypeMapperDelegateOneToOne((idExtractor, entityIds) ->
                        queryOneToOne((IDC) entityIds,
                                ruleMapperSource.apply(idExtractor),
                                idExtractor,
                                defaultResultProvider,
                                mapFactory),
                idCollectionFactory);
    }

    static <ID, R> RuleMapper<ID, R, List<R>> oneToMany(Function<List<ID>, Publisher<R>> queryFunction) {
        return oneToMany(call(queryFunction), ArrayList::new, (MapFactory<ID, List<R>>) null);
    }

    static <ID, R> RuleMapper<ID, R, List<R>> oneToMany(RuleMapperSource<ID, List<ID>, R> ruleMapperSource) {
        return oneToMany(ruleMapperSource, ArrayList::new, (MapFactory<ID, List<R>>) null);
    }

    static <ID, R> RuleMapper<ID, R, List<R>> oneToMany(
            Function<List<ID>, Publisher<R>> queryFunction,
            MapFactory<ID, List<R>> mapFactory) {
        return oneToMany(call(queryFunction), ArrayList::new, mapFactory);
    }

    static <ID, R> RuleMapper<ID, R, List<R>> oneToMany(
            RuleMapperSource<ID, List<ID>, R> ruleMapperSource,
            MapFactory<ID, List<R>> mapFactory) {
        return oneToMany(ruleMapperSource, ArrayList::new, mapFactory);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, R, List<R>> oneToMany(
            Function<IDC, Publisher<R>> queryFunction,
            Supplier<IDC> idCollectionFactory) {
        return oneToMany(call(queryFunction), ArrayList::new, idCollectionFactory);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, R, List<R>> oneToMany(
            RuleMapperSource<ID, IDC, R> ruleMapperSource,
            Supplier<IDC> idCollectionFactory) {
        return oneToMany(ruleMapperSource, ArrayList::new, idCollectionFactory);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, R, List<R>> oneToMany(
            Function<IDC, Publisher<R>> queryFunction,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, List<R>> mapFactory) {
        return oneToMany(call(queryFunction), ArrayList::new, idCollectionFactory, mapFactory);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, R, List<R>> oneToMany(
            RuleMapperSource<ID, IDC, R> ruleMapperSource,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, List<R>> mapFactory) {
        return oneToMany(ruleMapperSource, ArrayList::new, idCollectionFactory, mapFactory);
    }

    static <ID, R> RuleMapper<ID, R, Set<R>> oneToManyAsSet(Function<List<ID>, Publisher<R>> queryFunction) {
        return oneToManyAsSet(call(queryFunction), ArrayList::new, null);
    }

    static <ID, R> RuleMapper<ID, R, Set<R>> oneToManyAsSet(RuleMapperSource<ID, List<ID>, R> ruleMapperSource) {
        return oneToManyAsSet(ruleMapperSource, ArrayList::new, null);
    }

    static <ID, R> RuleMapper<ID, R, Set<R>> oneToManyAsSet(
            Function<List<ID>, Publisher<R>> queryFunction,
            MapFactory<ID, Set<R>> mapFactory) {
        return oneToManyAsSet(call(queryFunction), ArrayList::new, mapFactory);
    }

    static <ID, R> RuleMapper<ID, R, Set<R>> oneToManyAsSet(
            RuleMapperSource<ID, List<ID>, R> ruleMapperSource,
            MapFactory<ID, Set<R>> mapFactory) {
        return oneToManyAsSet(ruleMapperSource, ArrayList::new, mapFactory);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, R, Set<R>> oneToManyAsSet(
            Function<IDC, Publisher<R>> queryFunction,
            Supplier<IDC> idCollectionFactory) {
        return oneToMany(call(queryFunction), HashSet::new, idCollectionFactory);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, R, Set<R>> oneToManyAsSet(
            RuleMapperSource<ID, IDC, R> ruleMapperSource,
            Supplier<IDC> idCollectionFactory) {
        return oneToMany(ruleMapperSource, HashSet::new, idCollectionFactory);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, R, Set<R>> oneToManyAsSet(
            Function<IDC, Publisher<R>> queryFunction,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, Set<R>> mapFactory) {
        return oneToMany(call(queryFunction), HashSet::new, idCollectionFactory, mapFactory);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, R, Set<R>> oneToManyAsSet(
            RuleMapperSource<ID, IDC, R> ruleMapperSource,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, Set<R>> mapFactory) {
        return oneToMany(ruleMapperSource, HashSet::new, idCollectionFactory, mapFactory);
    }

    static <ID, IDC extends Collection<ID>, R, RC extends Collection<R>> RuleMapper<ID, R, RC> oneToMany(
            Function<IDC, Publisher<R>> queryFunction,
            Supplier<RC> collectionFactory,
            Supplier<IDC> idCollectionFactory) {
        return oneToMany(call(queryFunction), collectionFactory, idCollectionFactory, null);
    }

    static <ID, IDC extends Collection<ID>, R, RC extends Collection<R>> RuleMapper<ID, R, RC> oneToMany(
            RuleMapperSource<ID, IDC, R> ruleMapperSource,
            Supplier<RC> collectionFactory,
            Supplier<IDC> idCollectionFactory) {
        return oneToMany(ruleMapperSource, collectionFactory, idCollectionFactory, null);
    }

    static <ID, IDC extends Collection<ID>, R, RC extends Collection<R>> RuleMapper<ID, R, RC> oneToMany(
            Function<IDC, Publisher<R>> queryFunction,
            Supplier<RC> collectionFactory,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, RC> mapFactory) {
        return oneToMany(call(queryFunction), collectionFactory, idCollectionFactory, mapFactory);
    }

    @SuppressWarnings("unchecked")
    static <ID, IDC extends Collection<ID>, R, RC extends Collection<R>> RuleMapper<ID, R, RC> oneToMany(
            RuleMapperSource<ID, IDC, R> ruleMapperSource,
            Supplier<RC> collectionFactory,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, RC> mapFactory) {
        return convertIdTypeMapperDelegateOneToMany((idExtractor, entityIds) ->
                queryOneToMany((IDC) entityIds, ruleMapperSource.apply(idExtractor), idExtractor, collectionFactory, mapFactory), idCollectionFactory);
    }

    private static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, R, R> convertIdTypeMapperDelegateOneToOne(
            RuleMapper<ID, R, R> mapper, Supplier<IDC> idCollectionFactory) {
        return (idExtractor, entityIds) -> mapper.apply(idExtractor, refineEntityIDType(entityIds, idCollectionFactory));
    }

    private static <ID, IDC extends Collection<ID>, R, RC extends Collection<R>> RuleMapper<ID, R, RC> convertIdTypeMapperDelegateOneToMany(
            RuleMapper<ID, R, RC> mapper, Supplier<IDC> idCollectionFactory) {
        return (idExtractor, entityIds) -> mapper.apply(idExtractor, refineEntityIDType(entityIds, idCollectionFactory));
    }

    private static <ID, IDC extends Collection<ID>> IDC refineEntityIDType(Iterable<ID> entityIds, Supplier<IDC> idCollectionFactory) {
        return stream(entityIds.spliterator(), false)
                .collect(toCollection(idCollectionFactory));
    }
}
