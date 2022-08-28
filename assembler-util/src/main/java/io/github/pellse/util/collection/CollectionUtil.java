package io.github.pellse.util.collection;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.github.pellse.util.ObjectUtils.also;
import static io.github.pellse.util.ObjectUtils.ifNotNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Stream.concat;
import static java.util.stream.StreamSupport.stream;

public interface CollectionUtil {

    static <T, C extends Iterable<T>> Stream<T> toStream(C iterable) {
        return iterable != null ? stream(iterable.spliterator(), false) : Stream.empty();
    }

    static boolean isEmpty(Iterable<?> iterable) {
        return iterable == null ||
                (iterable instanceof Collection<?> coll && coll.isEmpty()) ||
                !iterable.iterator().hasNext();
    }

    static <T> T first(Iterable<T> coll) {
        return isNotEmpty(coll) ? coll.iterator().next() : null;
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
        return iter instanceof Collection<E> coll ? coll : stream(iter.spliterator(), false).toList();
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

    static <K, V> Map<K, V> diff(Map<K, V> map1, Map<K, V> map2) {
        return readAll(intersect(map1.keySet(), map2.keySet()), map1);
    }

    static <K, V> Map<K, V> readAll(Iterable<K> keys, Map<K, V> sourceMap) {
        return newMap(map -> keys.forEach(id -> ifNotNull(sourceMap.get(id), value -> map.put(id, value))));
    }

    static <T> Iterable<T> toIterable(Iterator<T> iterator) {
        return () -> iterator;
    }

    static <K, V, VC extends Collection<V>> VC removeDuplicates(
            Collection<V> coll,
            Function<? super V, K> keyExtractor,
            Supplier<VC> collectionFactory) {

        return removeDuplicates(toStream(coll), keyExtractor, collectionFactory);
    }

    static <K, V, VC extends Collection<V>> VC removeDuplicates(
            Stream<V> stream,
            Function<? super V, K> keyExtractor,
            Supplier<VC> collectionFactory) {

        var noDuplicateColl = stream
                .collect(toMap(keyExtractor, identity(), (o, o2) -> o2, LinkedHashMap::new))
                .values();

        return also(collectionFactory.get(), c -> c.addAll(noDuplicateColl));
    }

    static <K, V, VC extends Collection<V>, ID> Map<K, VC> removeDuplicates(
            Map<K, VC> map,
            Function<? super V, ID> idExtractor,
            Supplier<VC> collectionFactory) {

        return removeDuplicates(map, idExtractor, collectionFactory, true);
    }

    static <K, V, VC extends Collection<V>, ID> Map<K, VC> removeDuplicates(
            Map<K, VC> map,
            Function<? super V, ID> idExtractor,
            Supplier<VC> collectionFactory,
            boolean copyMap) {

        return also(copyMap ? new HashMap<>(map) : map, m -> m.replaceAll((id, coll) -> removeDuplicates(coll, idExtractor, collectionFactory)));
    }

    static <K, V, VC extends Collection<V>, ID> Map<K, VC> mergeMaps(
            Map<K, VC> srcMap,
            Map<K, VC> targetMap,
            Function<? super V, ID> idExtractor,
            Supplier<VC> collectionFactory) {

        return mergeMaps(srcMap, targetMap, idExtractor, collectionFactory, true);
    }

    static <K, V, VC extends Collection<V>, ID> Map<K, VC> mergeMaps(
            Map<K, VC> srcMap,
            Map<K, VC> targetMap,
            Function<? super V, ID> idExtractor,
            Supplier<VC> collectionFactory,
            boolean copyTargetMap) {

        var newTargetMap = copyTargetMap ? new HashMap<>(targetMap) : targetMap;

        newTargetMap.replaceAll((id, oldList) ->
                removeDuplicates(concat(toStream(oldList), toStream(srcMap.get(id))), idExtractor, collectionFactory));

        newTargetMap.putAll(removeDuplicates(diff(srcMap, newTargetMap), idExtractor, collectionFactory, false));
        return newTargetMap;
    }

    @SafeVarargs
    static <K, V> Map<K, V> mergeMaps(Map<K, V>... maps) {
        return Stream.of(maps)
                .flatMap(map -> map.entrySet().stream())
                .collect(toMap(Entry::getKey, Entry::getValue, (v1, v2) -> v1));
    }
}
