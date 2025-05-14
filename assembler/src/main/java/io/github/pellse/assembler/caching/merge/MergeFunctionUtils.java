package io.github.pellse.assembler.caching.merge;

import java.util.List;
import java.util.function.Function;

import static io.github.pellse.util.collection.CollectionUtils.asList;
import static java.lang.Math.max;
import static java.util.List.copyOf;

public interface MergeFunctionUtils {

    static <R> Function<List<R>, List<R>> keepFirst(int nbElements) {
        return keep(nbElements, list -> list.subList(0, nbElements));
    }

    static <R> Function<List<R>, List<R>> keepLast(int nbElements) {
        return keep(nbElements, list -> list.subList(max(0, list.size() - nbElements), list.size()));
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
