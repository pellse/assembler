package io.github.pellse.util.collection;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.github.pellse.util.ObjectUtils.also;
import static io.github.pellse.util.ObjectUtils.ifNotNull;
import static java.util.Map.entry;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.*;
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

    static <T, R> List<R>  transform(Iterable<? extends T> from, Function<T, R> mappingFunction) {
        return toStream(from).map(mappingFunction).toList();
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

    @SafeVarargs
    static <K, V> Map<K, V> newMap(Consumer<Map<K, V>>... initializers) {
        return newMap(null, initializers);
    }

    @SafeVarargs
    static <K, V> Map<K, V> newMap(Map<K, V> map, Consumer<Map<K, V>>... initializers) {
        final var copyMap = map != null ? new HashMap<>(map) : new HashMap<K, V>();

        for (var initializer : initializers) {
            initializer.accept(copyMap);
        }
        return copyMap;
    }

    static <K, V> Map<K, V> diff(Map<K, V> map1, Map<K, V> map2) {
        return readAll(intersect(map1.keySet(), map2.keySet()), map1);
    }

    static <K, V> Map<K, V> readAll(Iterable<K> keys, Map<K, V> sourceMap) {
        return newMap(map -> keys.forEach(id -> ifNotNull(sourceMap.get(id), value -> map.put(id, value))));
    }

    static <K, V, VC extends Collection<V>> VC removeDuplicates(
            Collection<V> coll,
            Function<? super V, K> keyExtractor,
            Supplier<VC> collectionFactory) {

        return removeDuplicates(toStream(coll), keyExtractor, collectionFactory);
    }

    private static <K, V, VC extends Collection<V>> VC removeDuplicates(
            Stream<V> stream,
            Function<? super V, K> keyExtractor,
            Supplier<VC> collectionFactory) {

        final var noDuplicateColl = stream
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

        final var newMap = copyMap ? new HashMap<>(map) : map;

        newMap.replaceAll((id, coll) -> removeDuplicates(coll, idExtractor, collectionFactory));
        return newMap;
    }

    static <K, V, ID> Map<K, List<V>> mergeMaps(
            Map<K, List<V>> srcMap,
            Map<K, List<V>> targetMap,
            Function<? super V, ID> idExtractor) {

        return mergeMaps(srcMap, targetMap, idExtractor, ArrayList::new);
    }

    static <K, V, VC extends Collection<V>, ID> Map<K, VC> mergeMaps(
            Map<K, VC> srcMap,
            Map<K, VC> targetMap,
            Function<? super V, ID> idExtractor,
            Supplier<VC> collectionFactory) {

        return newMap(targetMap,
                m -> m.replaceAll((id, oldList) -> removeDuplicates(concat(toStream(oldList), toStream(srcMap.get(id))), idExtractor, collectionFactory)),
                m -> m.putAll(removeDuplicates(diff(srcMap, m), idExtractor, collectionFactory, false)));
    }

    @SafeVarargs
    static <K, V> Map<K, V> mergeMaps(Map<K, V>... maps) {
        return Stream.of(maps)
                .flatMap(map -> map.entrySet().stream())
                .collect(toMap(Entry::getKey, Entry::getValue, (v1, v2) -> v1));
    }

    static <K, V, ID> Map<K, List<V>> subtractFromMap(
            Map<K, List<V>> mapToSubtract,
            Map<K, List<V>> srcMap,
            Function<? super V, ID> idExtractor) {

        return subtractFromMap(mapToSubtract, srcMap, idExtractor, ArrayList::new);
    }

    static <K, V, VC extends Collection<V>, ID> Map<K, VC> subtractFromMap(
            Map<K, VC> mapToSubtract,
            Map<K, VC> srcMap,
            Function<? super V, ID> idExtractor,
            Supplier<VC> collectionFactory) {

        return srcMap.entrySet().stream()
                .map(entry -> {
                    final var itemsToSubtract = mapToSubtract.get(entry.getKey());
                    if (itemsToSubtract == null)
                        return entry;

                    final var idsToSubtract = itemsToSubtract.stream()
                            .map(idExtractor)
                            .collect(toSet());

                    final var newColl = toStream(entry.getValue())
                            .filter(element -> !idsToSubtract.contains((idExtractor.apply(element))))
                            .collect(toCollection(collectionFactory));

                    return isNotEmpty(newColl) ? entry(entry.getKey(), newColl) : null;
                })
                .filter(Objects::nonNull)
                .collect(toMap(Entry::getKey, Entry::getValue, (v1, v2) -> v1));
    }
}
