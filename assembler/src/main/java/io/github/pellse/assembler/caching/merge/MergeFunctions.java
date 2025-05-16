package io.github.pellse.assembler.caching.merge;

import io.github.pellse.util.collection.CollectionUtils;

import java.util.List;
import java.util.function.Function;

import static io.github.pellse.util.collection.CollectionUtils.asList;
import static io.github.pellse.util.collection.CollectionUtils.concat;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.List.copyOf;

public interface MergeFunctions {

    static <ID, EID, R> MergeFunctionFactory<ID, EID, R> removeDuplicates() {
        return MergeFunctions::removeDuplicates;
    }

    static <ID, EID, R> MergeFunction<ID, List<R>> removeDuplicates(MergeFunctionContext<EID, R> ctx) {
        return removeDuplicates(ctx.idResolver());
    }

    static <ID, EID, R> MergeFunction<ID, List<R>> removeDuplicates(Function<R, EID> idResolver) {
        return (id, existingList, newList) -> CollectionUtils.removeDuplicates(concat(existingList, newList), idResolver);
    }

    static <EID, R> Function<MergeFunctionContext<EID, R>, Function<List<R>, List<R>>> removeAllDuplicates() {
        return MergeFunctions::removeAllDuplicates;
    }

    static <EID, R> Function<List<R>, List<R>> removeAllDuplicates(MergeFunctionContext<EID, R> ctx) {
        return list -> CollectionUtils.removeDuplicates(list, ctx.idResolver());
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

    static <R> Function<List<R>, List<R>> keepFirstN(int nbElements) {
        return keep(nbElements, list -> list.subList(0, nbElements));
    }

    static <R> Function<List<R>, List<R>> keepLastN(int nbElements) {
        return keep(nbElements, list -> list.subList(max(0, list.size() - nbElements), list.size()));
    }

    static <ID, R> MergeFunction<ID, R> replace() {
        return MergeFunctions::replace;
    }

    static <ID, R> R replace(ID id, R r1, R r2) {
        return r2 != null ? r2 : r1;
    }

    private static <R> Function<List<R>, List<R>> keep(int nbElements, Function<List<R>, List<R>> subListFunction) {
        return list -> {
            final var nonNullList = asList(list);

            if (nbElements <= 0) {
                return List.of();
            } else if (nonNullList.size() <= nbElements) {
                return nonNullList;
            }

            return copyOf(subListFunction.apply(nonNullList));
        };
    }
}