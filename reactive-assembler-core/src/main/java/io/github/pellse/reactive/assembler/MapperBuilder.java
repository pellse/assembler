package io.github.pellse.reactive.assembler;

import io.github.pellse.util.function.checked.CheckedFunction1;
import org.reactivestreams.Publisher;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

@FunctionalInterface
public interface MapperBuilder<ID, R, RC, EX extends Throwable> extends Function<Function<R, ID>, Mapper<ID, RC, EX>> {

    static <ID, R, EX extends Throwable> MapperBuilder<ID, R, R, EX> oneToOne(
            CheckedFunction1<List<ID>, Publisher<R>, EX> queryFunction) {

        return oneToOne(queryFunction, id -> null, ArrayList::new, null);
    }

    static <ID, IDC extends Collection<ID>, R, EX extends Throwable> MapperBuilder<ID, R, R, EX> oneToOne(
            CheckedFunction1<IDC, Publisher<R>, EX> queryFunction,
            Supplier<IDC> idCollectionFactory) {

        return oneToOne(queryFunction, id -> null, idCollectionFactory);
    }

    static <ID, R, EX extends Throwable> MapperBuilder<ID, R, R, EX> oneToOne(
            CheckedFunction1<List<ID>, Publisher<R>, EX> queryFunction,
            Function<ID, R> defaultResultProvider) {

        return oneToOne(queryFunction, defaultResultProvider, ArrayList::new, null);
    }

    static <ID, R, EX extends Throwable> MapperBuilder<ID, R, R, EX> oneToOne(
            CheckedFunction1<List<ID>, Publisher<R>, EX> queryFunction,
            Function<ID, R> defaultResultProvider,
            MapFactory<ID, R> mapFactory) {

        return oneToOne(queryFunction, defaultResultProvider, ArrayList::new, mapFactory);
    }

    static <ID, IDC extends Collection<ID>, R, EX extends Throwable> MapperBuilder<ID, R, R, EX> oneToOne(
            CheckedFunction1<IDC, Publisher<R>, EX> queryFunction,
            Function<ID, R> defaultResultProvider,
            Supplier<IDC> idCollectionFactory) {

        return oneToOne(queryFunction, defaultResultProvider, idCollectionFactory, null);
    }

    static <ID, IDC extends Collection<ID>, R, EX extends Throwable> MapperBuilder<ID, R, R, EX> oneToOne(
            CheckedFunction1<IDC, Publisher<R>, EX> queryFunction,
            Function<ID, R> defaultResultProvider,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, R> mapFactory) {
        return idExtractor -> Mapper.oneToOne(queryFunction, idExtractor, defaultResultProvider, idCollectionFactory, mapFactory);
    }

    static <ID, R, EX extends Throwable> MapperBuilder<ID, R, List<R>, EX> oneToManyAsList(
            CheckedFunction1<List<ID>, Publisher<R>, EX> queryFunction) {

        return oneToManyAsList(queryFunction, ArrayList::new, null);
    }

    static <ID, R, EX extends Throwable> MapperBuilder<ID, R, List<R>, EX> oneToManyAsList(
            CheckedFunction1<List<ID>, Publisher<R>, EX> queryFunction,
            MapFactory<ID, List<R>> mapFactory) {

        return oneToManyAsList(queryFunction, ArrayList::new, mapFactory);
    }

    static <ID, IDC extends Collection<ID>, R, EX extends Throwable> MapperBuilder<ID, R, List<R>, EX> oneToManyAsList(
            CheckedFunction1<IDC, Publisher<R>, EX> queryFunction,
            Supplier<IDC> idCollectionFactory) {

        return oneToMany(queryFunction, ArrayList::new, idCollectionFactory);
    }

    static <ID, IDC extends Collection<ID>, R, EX extends Throwable> MapperBuilder<ID, R, List<R>, EX> oneToManyAsList(
            CheckedFunction1<IDC, Publisher<R>, EX> queryFunction,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, List<R>> mapFactory) {

        return oneToMany(queryFunction, ArrayList::new, idCollectionFactory, mapFactory);
    }

    static <ID, R, EX extends Throwable> MapperBuilder<ID, R, Set<R>, EX> oneToManyAsSet(
            CheckedFunction1<Set<ID>, Publisher<R>, EX> queryFunction) {

        return oneToManyAsSet(queryFunction, HashSet::new, null);
    }

    static <ID, R, EX extends Throwable> MapperBuilder<ID, R, Set<R>, EX> oneToManyAsSet(
            CheckedFunction1<Set<ID>, Publisher<R>, EX> queryFunction,
            MapFactory<ID, Set<R>> mapFactory) {

        return oneToManyAsSet(queryFunction, HashSet::new, mapFactory);
    }

    static <ID, IDC extends Collection<ID>, R, EX extends Throwable> MapperBuilder<ID, R, Set<R>, EX> oneToManyAsSet(
            CheckedFunction1<IDC, Publisher<R>, EX> queryFunction,
            Supplier<IDC> idCollectionFactory) {

        return oneToMany(queryFunction, HashSet::new, idCollectionFactory);
    }

    static <ID, IDC extends Collection<ID>, R, EX extends Throwable> MapperBuilder<ID, R, Set<R>, EX> oneToManyAsSet(
            CheckedFunction1<IDC, Publisher<R>, EX> queryFunction,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, Set<R>> mapFactory) {

        return oneToMany(queryFunction, HashSet::new, idCollectionFactory, mapFactory);
    }

    static <ID, IDC extends Collection<ID>, R, RC extends Collection<R>, EX extends Throwable> MapperBuilder<ID, R, RC, EX> oneToMany(
            CheckedFunction1<IDC, Publisher<R>, EX> queryFunction,
            Supplier<RC> collectionFactory,
            Supplier<IDC> idCollectionFactory) {

        return oneToMany(queryFunction, collectionFactory, idCollectionFactory, null);
    }

    static <ID, IDC extends Collection<ID>, R, RC extends Collection<R>, EX extends Throwable> MapperBuilder<ID, R, RC, EX> oneToMany(
            CheckedFunction1<IDC, Publisher<R>, EX> queryFunction,
            Supplier<RC> collectionFactory,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, RC> mapFactory) {

        return idExtractor -> Mapper.oneToMany(queryFunction, idExtractor, collectionFactory, idCollectionFactory, mapFactory);
    }
}
