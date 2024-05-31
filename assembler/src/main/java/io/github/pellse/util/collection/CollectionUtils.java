/*
 * Copyright 2023 Sebastien Pelletier
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.pellse.util.collection;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.github.pellse.util.ObjectUtils.also;
import static io.github.pellse.util.ObjectUtils.ifNotNull;
import static java.util.Collections.emptySet;
import static java.util.Map.entry;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.*;
import static java.util.stream.StreamSupport.stream;

public interface CollectionUtils {

    static <T, C extends Iterable<T>> Stream<T> toStream(C iterable) {
        return iterable != null ? stream(iterable.spliterator(), false) : Stream.empty();
    }

    static <T, C extends Iterable<T>> Stream<T> concat(C collection1, C collection2) {
        return Stream.concat(toStream(collection1), toStream(collection2));
    }

    static boolean isEmpty(Iterable<?> iterable) {

        return iterable == null ||
                (iterable instanceof Collection<?> coll && coll.isEmpty()) ||
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

    static <K, V> Map<K, V> nullToEmptyMap(Map<K, V> value) {
        return nullToEmptyMap(value, Map::of);
    }

    static <K, V, T extends Map<K, V>> T nullToEmptyMap(T value, Supplier<T> defaultFactory) {
        return value != null ? value : defaultFactory.get();
    }

    static <T, R> List<R> transform(Iterable<? extends T> from, Function<T, R> mappingFunction) {
        return toStream(from).map(mappingFunction).toList();
    }

    static <E> Collection<E> asCollection(Iterable<E> iterable) {
        return iterable instanceof Collection<E> coll ? coll : stream(iterable.spliterator(), false).toList();
    }

    static <E, C extends Collection<E>> C translate(Iterable<? extends E> from, Supplier<C> collectionFactory) {
        return asCollection(from).stream().collect(toCollection(collectionFactory));
    }

    static long size(Iterable<?> iterable) {
        return iterable == null ? 0 : asCollection(iterable).size();
    }

    static <E> Set<E> intersect(Iterable<? extends E> iterable1, Iterable<? extends E> iterable2) {
        final var coll1 = asCollection(iterable1);
        if (coll1.isEmpty()) {
            return emptySet();
        }

        final var finalSet = new HashSet<E>(coll1);

        final var coll2 = asCollection(iterable2);
        if (coll2.isEmpty()) {
            return finalSet;
        }

        return also(finalSet, set -> set.removeAll(coll2));
    }

    static <K, V, V1> Map<K, V1> transformMap(Map<K, V> map, Function<V, V1> mappingFunction) {
        return transformMap(map, (__, value) -> mappingFunction.apply(value));
    }

    static <K, V, V1> Map<K, V1> transformMap(Map<K, V> map, BiFunction<K, V, V1> mappingFunction) {
        return map.entrySet().stream()
                .collect(toMap(Entry::getKey, e -> mappingFunction.apply(e.getKey(), e.getValue()), (v1, v2) -> v2, LinkedHashMap::new));
    }

    @SafeVarargs
    static <K, V> Map<K, V> newMap(Consumer<Map<K, V>>... initializers) {
        return newMap(null, initializers);
    }

    @SafeVarargs
    static <K, V> Map<K, V> newMap(Map<K, V> map, Consumer<Map<K, V>>... initializers) {
        return newMap(map, LinkedHashMap::new, initializers);
    }

    @SafeVarargs
    static <K, V, M extends Map<K, V>> M newMap(Map<K, V> map, Supplier<M> mapSupplier, Consumer<Map<K, V>>... initializers) {

        final var copyMap = mapSupplier.get();
        if (map != null) {
            copyMap.putAll(map);
        }

        for (var initializer : initializers) {
            initializer.accept(copyMap);
        }
        return copyMap;
    }

    static <K, V> Map<K, V> diff(Map<K, V> map1, Map<K, V> map2) {
        return readAll(intersect(map1.keySet(), map2.keySet()), map1);
    }

    static <K, V> Map<K, V> readAll(Iterable<K> keys, Map<K, V> sourceMap) {
        return readAll(keys, sourceMap, identity());
    }

    static <K, V, V1> Map<K, V1> readAll(Iterable<K> keys, Map<K, V> sourceMap, Function<V, V1> mappingFunction) {
        return readAll(keys, sourceMap, LinkedHashMap::new, mappingFunction);
    }

    static <K, V, V1, M extends Map<K, V1>> Map<K, V1> readAll(Iterable<K> keys, Map<K, V> sourceMap, Supplier<M> mapSupplier, Function<V, V1> mappingFunction) {
        return newMap(null, mapSupplier, map -> keys.forEach(key -> ifNotNull(sourceMap.get(key), value -> map.put(key, mappingFunction.apply(value)))));
    }

    static <K, V, VC extends Collection<V>> VC removeDuplicates(
            Collection<V> coll,
            Function<? super V, K> keyExtractor,
            Function<Collection<V>, VC> collectionConverter) {

        return removeDuplicates(toStream(coll), keyExtractor, collectionConverter);
    }

    private static <K, V, VC extends Collection<V>> VC removeDuplicates(
            Stream<V> stream,
            Function<? super V, K> keyExtractor,
            Function<Collection<V>, VC> collectionConverter) {

        return collectionConverter.apply(stream
                .collect(toMap(keyExtractor, identity(), (v1, v2) -> v2, LinkedHashMap::new))
                .values());
    }

    static <K, V> LinkedHashMap<K, V> toLinkedHashMap(Map<K, V> map) {
        return map instanceof LinkedHashMap<K, V> lhm ? lhm : new LinkedHashMap<>(map);
    }

    static <T, K, V> LinkedHashMap<K, V> toLinkedHashMap(Iterable<T> iterable, Function<T, K> keyExtractor, Function<T, V> valueExtractor) {
        return toStream(iterable).collect(toMap(keyExtractor, valueExtractor, (v1, v2) -> v2, LinkedHashMap::new));
    }

    static <K, V, ID> Map<K, List<V>> mergeMaps(
            Map<K, List<V>> existingMap,
            Map<K, List<V>> newMap,
            Function<? super V, ID> idResolver) {

        return mergeMaps(existingMap, newMap, idResolver, ArrayList::new);
    }

    static <K, V, VC extends Collection<V>, ID> Map<K, VC> mergeMaps(
            Map<K, VC> existingMap,
            Map<K, VC> newMap,
            Function<? super V, ID> idResolver,
            Function<Collection<V>, VC> collectionConverter) {

        return concat(existingMap.entrySet(), newMap.entrySet())
                .map(entry -> entry(entry.getKey(), removeDuplicates(entry.getValue(), idResolver, collectionConverter)))
                .collect(toMap(
                        Entry::getKey,
                        Entry::getValue,
                        (coll1, coll2) -> removeDuplicates(concat(coll1, coll2), idResolver, collectionConverter),
                        LinkedHashMap::new));
    }

    @SafeVarargs
    static <K, V> Map<K, V> mergeMaps(Map<K, V>... maps) {
        return Stream.of(maps)
                .flatMap(map -> map.entrySet().stream())
                .collect(toMap(Entry::getKey, Entry::getValue, (v1, v2) -> v2, LinkedHashMap::new));
    }

    static <K, V, ID> Map<K, List<V>> subtractFromMap(
            Map<K, List<V>> mapToSubtract,
            Map<K, List<V>> srcMap,
            Function<? super V, ID> idResolver) {

        return subtractFromMap(mapToSubtract, srcMap, idResolver, ArrayList::new);
    }

    static <K, V, VC extends Collection<V>, ID> Map<K, VC> subtractFromMap(
            Map<K, VC> mapToSubtract,
            Map<K, VC> srcMap,
            Function<? super V, ID> idResolver,
            Supplier<VC> collectionFactory) {

        return srcMap.entrySet().stream()
                .map(entry -> {
                    final var itemsToSubtract = mapToSubtract.get(entry.getKey());
                    if (itemsToSubtract == null)
                        return entry;

                    final var idsToSubtract = itemsToSubtract.stream()
                            .map(idResolver)
                            .collect(toSet());

                    final var newColl = toStream(entry.getValue())
                            .filter(element -> !idsToSubtract.contains((idResolver.apply(element))))
                            .collect(toCollection(collectionFactory));

                    return isNotEmpty(newColl) ? entry(entry.getKey(), newColl) : null;
                })
                .filter(Objects::nonNull)
                .collect(toMap(Entry::getKey, Entry::getValue, (v1, v2) -> v1, LinkedHashMap::new));
    }
}
