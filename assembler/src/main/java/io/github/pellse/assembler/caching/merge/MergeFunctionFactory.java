package io.github.pellse.assembler.caching.merge;

import io.github.pellse.assembler.RuleMapperContext;
import io.github.pellse.assembler.RuleMapperContext.OneToOneContext;

import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.function.Function.identity;

public interface MergeFunctionFactory <T, K, ID, EID, R, RRC, CTX extends RuleMapperContext<T, K, ID, EID, R, RRC>> extends Function<CTX, MergeFunction<ID, RRC>> {

    MergeFunction<ID, RRC> create(CTX ctx);

    @Override
    default MergeFunction<ID, RRC> apply(CTX ctx) {
        return create(ctx);
    }

    default  MergeFunctionFactory<T, K, ID, EID, R, RRC, CTX> pipe(Function<RRC, RRC> finisher) {
        return pipeWith(__ -> finisher);
    }

    default  MergeFunctionFactory<T, K, ID, EID, R, RRC, CTX> pipeWith(Function<CTX, Function<RRC, RRC>> finisherFactory) {
        return ctx -> {
            final var mergeFunction = create(ctx);
            final var finisher = finisherFactory.apply(ctx);

            return (id, existing, next) -> finisher.apply(mergeFunction.apply(id, existing, next));
        };
    }

    static <T, K, ID, EID, R, RRC, CTX extends RuleMapperContext<T, K, ID, EID, R, RRC>> MergeFunctionFactory<T, K, ID, EID, R, RRC, CTX> with(MergeFunctionFactory<T, K, ID, EID, R, RRC, CTX> mergeFunctionFactory) {
        return mergeFunctionFactory;
    }

    static <T, K, ID, EID, R, RRC, CTX extends RuleMapperContext<T, K, ID, EID, R, RRC>> MergeFunctionFactory<T, K, ID, EID, R, RRC, CTX> with(MergeFunction<ID, RRC> mergeFunction) {
        return __ -> mergeFunction;
    }

    static <T, K, ID, R> MergeFunctionFactory<T, K, ID, ID, R, R, OneToOneContext<T, K, ID, R>> withOneToOne(MergeFunction<ID, R> mergeFunction) {
        return __ -> mergeFunction;
    }

    @SafeVarargs
    static <T, K, ID, EID, R, RRC, CTX extends RuleMapperContext<T, K, ID, EID, R, RRC>> MergeFunctionFactory<T, K, ID, EID, R, RRC, CTX> pipe(
            MergeFunctionFactory<T, K, ID, EID, R, RRC, CTX> mergeFunctionFactory,
            Function<RRC, RRC>... finishers) {

        final var finisher = Stream.of(finishers)
                .reduce(identity(), (f, acc) -> acc.andThen(f));

        return mergeFunctionFactory.pipe(finisher);
    }

    @SafeVarargs
    static <T, K, ID, EID, R, RRC, CTX extends RuleMapperContext<T, K, ID, EID, R, RRC>> MergeFunctionFactory<T, K, ID, EID, R, RRC, CTX> pipeWith(
            MergeFunctionFactory<T, K, ID, EID, R, RRC, CTX> mergeFunctionFactory,
            Function<CTX, Function<RRC, RRC>>... finisherFactories) {

        return ctx -> {
            final var finisher = Stream.of(finisherFactories)
                    .map(f -> f.apply(ctx))
                    .reduce(identity(), (f, acc) -> acc.andThen(f));

            return mergeFunctionFactory.pipe(finisher).create(ctx);
        };
    }
}