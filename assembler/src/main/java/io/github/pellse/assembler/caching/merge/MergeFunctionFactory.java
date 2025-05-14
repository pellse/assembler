package io.github.pellse.assembler.caching.merge;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.function.Function.identity;

public interface MergeFunctionFactory <ID, EID, R> extends Function<MergeFunctionContext<EID, R>, MergeFunction<ID, List<R>>> {

    MergeFunction<ID, List<R>> create(MergeFunctionContext<EID, R> ctx);

    @Override
    default MergeFunction<ID, List<R>> apply(MergeFunctionContext<EID, R> ctx) {
        return create(ctx);
    }

    default  MergeFunctionFactory<ID, EID, R> pipe(Function<List<R>, List<R>> finisher) {
        return pipeWith(__ -> finisher);
    }

    default  MergeFunctionFactory<ID, EID, R> pipeWith(Function<MergeFunctionContext<EID, R>, Function<List<R>, List<R>>> finisherFactory) {
        return ctx -> {
            final var mergeFunction = create(ctx);
            final var finisher = finisherFactory.apply(ctx);

            return (id, existing, next) -> finisher.apply(mergeFunction.apply(id, existing, next));
        };
    }

    static <ID, EID, R> MergeFunctionFactory<ID, EID, R> from(MergeFunctionFactory<ID, EID, R> mergeFunctionFactory) {
        return mergeFunctionFactory;
    }

    static <ID, EID, R> MergeFunctionFactory<ID, EID, R> from(MergeFunction<ID, List<R>> mergeFunction) {
        return __ -> mergeFunction;
    }

    @SafeVarargs
    static <ID, EID, R> MergeFunctionFactory<ID, EID, R> pipe(
            MergeFunction<ID, List<R>> mergeFunction,
            Function<List<R>, List<R>>... finishers) {

        return pipe(from(mergeFunction), finishers);
    }

    @SafeVarargs
    static <ID, EID, R> MergeFunctionFactory<ID, EID, R> pipe(
            MergeFunctionFactory<ID, EID, R> mergeFunctionFactory,
            Function<List<R>, List<R>>... finishers) {

        final var finisher = Stream.of(finishers)
                .reduce(identity(), (f, acc) -> acc.andThen(f));

        return mergeFunctionFactory.pipe(finisher);
    }

    @SafeVarargs
    static <ID, EID, R> MergeFunctionFactory<ID, EID, R> pipeWith(
            MergeFunction<ID, List<R>> mergeFunction,
            Function<MergeFunctionContext<EID, R>, Function<List<R>, List<R>>>... finisherFactories) {

        return pipeWith(from(mergeFunction), finisherFactories);
    }

    @SafeVarargs
    static <ID, EID, R> MergeFunctionFactory<ID, EID, R> pipeWith(
            MergeFunctionFactory<ID, EID, R> mergeFunctionFactory,
            Function<MergeFunctionContext<EID, R>, Function<List<R>, List<R>>>... finisherFactories) {

        return ctx -> {
            final var finisher = Stream.of(finisherFactories)
                    .map(f -> f.apply(ctx))
                    .reduce(identity(), (f, acc) -> acc.andThen(f));

            return mergeFunctionFactory.pipe(finisher).create(ctx);
        };
    }
}