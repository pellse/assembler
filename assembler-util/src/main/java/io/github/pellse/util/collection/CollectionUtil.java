package io.github.pellse.util.collection;

import java.util.Collection;
import java.util.stream.Stream;

import static java.util.stream.StreamSupport.stream;

public interface CollectionUtil {

    static <T, C extends Iterable<T>> Stream<T> toStream(C iterable) {
        return iterable != null ? stream(iterable.spliterator(), false) : Stream.empty();
    }

    static boolean isEmpty(Iterable<?> iterable) {
        return iterable == null ||
                (iterable instanceof Collection && ((Collection<?>) iterable).isEmpty()) ||
                !iterable.iterator().hasNext();
    }

    static boolean isNotEmpty(Iterable<?> iterable) {
        return !isEmpty(iterable);
    }

    static long size(Iterable<?> iterable) {
        if (iterable == null)
            return 0;

        return iterable instanceof Collection
                ? ((Collection<?>) iterable).size()
                : stream(iterable.spliterator(), false).count();
    }
}
