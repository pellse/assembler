package io.github.pellse.util.collection;

import java.util.Collection;
import java.util.stream.Stream;

import static java.util.stream.StreamSupport.stream;

public interface CollectionUtil {

    static <T, C extends Iterable<T>> Stream<T> toStream(C iterable) {
        return iterable != null ? stream(iterable.spliterator(), false) : Stream.empty();
    }

    static boolean isEmpty(Collection<?> collection) {
        return collection == null || collection.isEmpty();
    }

    static boolean isNotEmpty(Collection<?> collection) {
        return !isEmpty(collection);
    }
}
