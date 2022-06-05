package io.github.pellse.util.collection;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.github.pellse.util.ObjectUtils.also;
import static io.github.pellse.util.ObjectUtils.ifNotNull;
import static java.util.stream.Collectors.toCollection;
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

    static boolean isEmpty(Map<?, ?> map) {
        return map == null || map.isEmpty();
    }

    static boolean isNotEmpty(Map<?, ?> map) {
        return !isEmpty(map);
    }

    static <E> Collection<E> asCollection(Iterable<E> iter) {
        return iter instanceof Collection ? (Collection<E>) iter : stream(iter.spliterator(), false).toList();
    }

    static <E, C extends Collection<E>> C translate(Iterable<? extends E> from, Supplier<C> collectionFactory) {
        return asCollection(from).stream().collect(toCollection(collectionFactory));
    }

    static long size(Iterable<?> iterable) {
        return iterable == null ? 0 : asCollection(iterable).size();
    }

    static <E> Set<E> intersect(Iterable<? extends E> iter1, Iterable<? extends E> iter2) {
        return also(new HashSet<>(asCollection(iter1)), set -> set.removeAll(asCollection(iter2)));
    }

    static <K, V> Map<K, V> newMap(Consumer<Map<K, V>> initializer) {
        return newMap(null, initializer);
    }

    static <K, V> Map<K, V> newMap(Map<K, V> map, Consumer<Map<K, V>> initializer) {
        final var copyMap = map != null ? new HashMap<>(map) : new HashMap<K, V>();
        initializer.accept(copyMap);
        return copyMap;
    }

    static <K, V> Map<K, V> readAll(Iterable<K> keys, Map<K, V> sourceMap) {
        return newMap(map -> keys.forEach(id -> ifNotNull(sourceMap.get(id), value -> map.put(id, value))));
    }

    @SafeVarargs
    static <K, V> Map<K, V> mergeMaps(Map<K, V>... maps) {
        return Stream.of(maps)
                .flatMap(map -> map.entrySet().stream())
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1));
    }
}
