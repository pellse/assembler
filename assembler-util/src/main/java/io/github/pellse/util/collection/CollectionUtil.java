package io.github.pellse.util.collection;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.github.pellse.util.ObjectUtils.also;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.StreamSupport.stream;

public interface CollectionUtil {

    static <T, C extends Iterable<T>> Stream<T> toStream(C iterable) {
        return iterable != null ? stream(iterable.spliterator(), false) : Stream.empty();
    }

    static boolean isEmpty(Iterable<?> iterable) {
        return iterable == null ||
                (iterable instanceof Collection coll && coll.isEmpty()) ||
                !iterable.iterator().hasNext();
    }

    static boolean isNotEmpty(Iterable<?> iterable) {
        return !isEmpty(iterable);
    }

    static <E> Collection<E> asCollection(Iterable<E> iter) {
        return iter instanceof Collection ? (Collection<E>) iter : stream(iter.spliterator(), false).toList();
    }

    @SuppressWarnings("unchecked")
    static <E> List<E> asList(E item) {
        return item instanceof Iterable ? asList((Iterable<E>) item) : List.of(item);
    }

    static <E> List<E> asList(Iterable<E> iter) {
        return iter instanceof List ? (List<E>) iter : stream(iter.spliterator(), false).toList();
    }

    static <E, C extends Collection<E>> C translate(Iterable<E> from, Supplier<C> collectionFactory) {
        return asCollection(from).stream().collect(Collectors.toCollection(collectionFactory));
    }

    static <E, C extends Collection<E>> C translate(Collection<E> from, Supplier<C> collectionFactory) {
        return from.stream().collect(Collectors.toCollection(collectionFactory));
    }

    static long size(Iterable<?> iterable) {
        if (iterable == null)
            return 0;

        return asCollection(iterable).size();
    }

    static <E, C extends Collection<E>> Set<E> intersect(C coll1, C coll2) {
        return also(new HashSet<>(coll1), set -> set.removeAll(coll2));
    }

    @SafeVarargs
    static <K, V> Map<K, V> mergeMaps(Map<K, V>... maps) {
        return Stream.of(maps)
                .flatMap(map -> map.entrySet().stream())
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1));
    }
}
