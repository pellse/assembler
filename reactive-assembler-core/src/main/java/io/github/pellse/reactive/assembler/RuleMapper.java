package io.github.pellse.reactive.assembler;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.github.pellse.reactive.assembler.QueryUtils.queryOneToMany;
import static io.github.pellse.reactive.assembler.QueryUtils.queryOneToOne;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.StreamSupport.stream;

@FunctionalInterface
public interface RuleMapper<ID, T, R> extends BiFunction<Function<T, ID>, Iterable<ID>, Mono<Map<ID, R>>> {

    static <ID, R> RuleMapper<ID, R, R> oneToOne(Function<List<ID>, Publisher<R>> queryFunction) {
        return oneToOne(queryFunction, id -> null, ArrayList::new, null);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, R, R> oneToOne(
            Function<IDC, Publisher<R>> queryFunction,
            Supplier<IDC> idCollectionFactory) {
        return oneToOne(queryFunction, id -> null, idCollectionFactory);
    }

    static <ID, R> RuleMapper<ID, R, R> oneToOne(
            Function<List<ID>, Publisher<R>> queryFunction,
            Function<ID, R> defaultResultProvider) {
        return oneToOne(queryFunction, defaultResultProvider, ArrayList::new, null);
    }

    static <ID, R> RuleMapper<ID, R, R> oneToOne(
            Function<List<ID>, Publisher<R>> queryFunction,
            Function<ID, R> defaultResultProvider,
            MapFactory<ID, R> mapFactory) {
        return oneToOne(queryFunction, defaultResultProvider, ArrayList::new, mapFactory);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, R, R> oneToOne(
            Function<IDC, Publisher<R>> queryFunction,
            Function<ID, R> defaultResultProvider,
            Supplier<IDC> idCollectionFactory) {
        return oneToOne(queryFunction, defaultResultProvider, idCollectionFactory, null);
    }

    @SuppressWarnings("unchecked")
    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, R, R> oneToOne(
            Function<IDC, Publisher<R>> queryFunction,
            Function<ID, R> defaultResultProvider,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, R> mapFactory) {
        return convertIdTypeMapperDelegateOneToOne((idExtractor, entityIds) ->
                queryOneToOne((IDC) entityIds, queryFunction, idExtractor, defaultResultProvider, mapFactory), idCollectionFactory);
    }

    static <ID, R> RuleMapper<ID, R, List<R>> oneToMany(Function<List<ID>, Publisher<R>> queryFunction) {
        return oneToMany(queryFunction, ArrayList::new, (MapFactory<ID, List<R>>) null);
    }

    static <ID, R> RuleMapper<ID, R, List<R>> oneToMany(
            Function<List<ID>, Publisher<R>> queryFunction,
            MapFactory<ID, List<R>> mapFactory) {
        return oneToMany(queryFunction, ArrayList::new, mapFactory);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, R, List<R>> oneToMany(
            Function<IDC, Publisher<R>> queryFunction,
            Supplier<IDC> idCollectionFactory) {
        return oneToMany(queryFunction, ArrayList::new, idCollectionFactory);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, R, List<R>> oneToMany(
            Function<IDC, Publisher<R>> queryFunction,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, List<R>> mapFactory) {
        return oneToMany(queryFunction, ArrayList::new, idCollectionFactory, mapFactory);
    }

    static <ID, R> RuleMapper<ID, R, Set<R>> oneToManyAsSet(Function<List<ID>, Publisher<R>> queryFunction) {
        return oneToManyAsSet(queryFunction, ArrayList::new, null);
    }

    static <ID, R> RuleMapper<ID, R, Set<R>> oneToManyAsSet(
            Function<List<ID>, Publisher<R>> queryFunction,
            MapFactory<ID, Set<R>> mapFactory) {
        return oneToManyAsSet(queryFunction, ArrayList::new, mapFactory);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, R, Set<R>> oneToManyAsSet(
            Function<IDC, Publisher<R>> queryFunction,
            Supplier<IDC> idCollectionFactory) {
        return oneToMany(queryFunction, HashSet::new, idCollectionFactory);
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapper<ID, R, Set<R>> oneToManyAsSet(
            Function<IDC, Publisher<R>> queryFunction,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, Set<R>> mapFactory) {
        return oneToMany(queryFunction, HashSet::new, idCollectionFactory, mapFactory);
    }

    static <ID, IDC extends Collection<ID>, R, RC extends Collection<R>> RuleMapper<ID, R, RC> oneToMany(
            Function<IDC, Publisher<R>> queryFunction,
            Supplier<RC> collectionFactory,
            Supplier<IDC> idCollectionFactory) {
        return oneToMany(queryFunction, collectionFactory, idCollectionFactory, null);
    }

    @SuppressWarnings("unchecked")
    static <ID, IDC extends Collection<ID>, R, RC extends Collection<R>> RuleMapper<ID, R, RC> oneToMany(
            Function<IDC, Publisher<R>> queryFunction,
            Supplier<RC> collectionFactory,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, RC> mapFactory) {
        return convertIdTypeMapperDelegateOneToMany((idExtractor, entityIds) ->
                queryOneToMany((IDC) entityIds, queryFunction, idExtractor, collectionFactory, mapFactory), idCollectionFactory);
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
