package io.github.pellse.assembler.caching.merge;

import io.github.pellse.assembler.RuleMapperContext.OneToManyContext;

import java.util.Comparator;
import java.util.function.Function;

public interface MergeFunctionContext<EID, R> {
    Function<R, EID> idResolver();

    Comparator<R> idComparator();

    static <T, K, ID, EID, R> MergeFunctionContext<EID, R> mergeFunctionContext(OneToManyContext<T, K, ID, EID, R> ctx) {
        return new MergeFunctionContext<>() {

            @Override
            public Function<R, EID> idResolver() {
                return ctx.idResolver();
            }

            @Override
            public Comparator<R> idComparator() {
                return ctx.idComparator();
            }
        };
    }
}
