package io.github.pellse.assembler.caching.merge;

import io.github.pellse.assembler.RuleMapperContext.OneToManyContext;
import io.github.pellse.util.collection.CollectionUtils;

import java.util.List;
import java.util.function.Function;

import static io.github.pellse.util.collection.CollectionUtils.*;
import static io.github.pellse.util.collection.CollectionUtils.concat;
import static java.lang.Math.min;

public interface MergeFunctions {

    static <T, K, ID, EID, R> MergeFunctionFactory<T, K, ID, EID, R, List<R>, OneToManyContext<T, K, ID, EID, R>> removeDuplicates() {
        return ctx -> removeDuplicates(ctx.idResolver());
    }

    static <ID, EID, R> MergeFunction<ID, List<R>> removeDuplicates(Function<R, EID> idResolver) {
        return (id, existingList, newList) -> CollectionUtils.removeDuplicates(concat(existingList, newList), idResolver);
    }

    static <ID, R> MergeFunction<ID, List<R>> keepFirst(int nbElements) {
        return (id, existingList, newList) -> concat(existingList, newList)
                .limit(min(nbElements, asList(existingList).size() + asList(newList).size()))
                .toList();
    }

    static <ID, R> MergeFunction<ID, List<R>> keepLast(int nbElements) {
        return (id, existingList, newList) -> {
            final var size = asList(existingList).size() + asList(newList).size();

            return concat(existingList, newList)
                    .skip(size - min(nbElements, size))
                    .toList();
        };
    }

    static <ID, R> MergeFunction<ID, R> replace() {
        return (k, r1, r2) -> r2 != null ? r2 : r1;
    }
}