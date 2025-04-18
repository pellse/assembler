/*
 * Copyright 2024 Sebastien Pelletier
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

import io.github.pellse.util.function.Function3;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.*;
import java.util.stream.Stream;

import static io.github.pellse.util.ObjectUtils.also;
import static io.github.pellse.util.ObjectUtils.ifNotNull;
import static java.util.Collections.emptySet;
import static java.util.LinkedHashMap.newLinkedHashMap;
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
        return iterable != null ? (iterable instanceof Collection<E> coll ? coll : stream(iterable.spliterator(), false).toList()) : List.of();
    }

    static <E, C extends Collection<E>> C translate(Iterable<? extends E> from, Supplier<C> collectionFactory) {
        return asCollection(from).stream().collect(toCollection(collectionFactory));
    }

    static int size(Iterable<?> iterable) {
        return iterable == null ? 0 : asCollection(iterable).size();
    }

    static <K, K1, V> Map<K1, V> transformMapKeys(Map<K, V> map, Function<K, K1> keyMapper) {
        return transformMapKeys(map, (key, __) -> keyMapper.apply(key));
    }

    static <K, K1, V> Map<K1, V> transformMapKeys(Map<K, V> map, BiFunction<K, V, K1> keyMapper) {
        return transformMap(map, keyMapper, (key, value) -> value);
    }

    static <K, V, V1> Map<K, V1> transformMapValues(Map<K, V> map, Function<V, V1> valueMapper) {
        return transformMapValues(map, (__, value) -> valueMapper.apply(value));
    }

    static <K, V, V1> Map<K, V1> transformMapValues(Map<K, V> map, BiFunction<K, V, V1> valueMapper) {
        return transformMap(map, (key, value) -> key, valueMapper);
    }

    static <K, V, K1, V1> Map<K1, V1> transformMap(Map<K, V> map, BiFunction<K, V, K1> keyMapper, BiFunction<K, V, V1> valueMapper) {
        return toLinkedHashMap(map.entrySet(), e -> keyMapper.apply(e.getKey(), e.getValue()), e -> valueMapper.apply(e.getKey(), e.getValue()));
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

    static <E> Set<E> diff(Iterable<? extends E> iterable1, Iterable<? extends E> iterable2) {

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

    static <K, V> Map<K, V> diff(Map<K, V> map1, Map<K, V> map2) {
        return readAll(diff(map1.keySet(), map2.keySet()), map1);
    }

    static <K, V> Map<K, V> readAll(Iterable<K> keys, Map<K, V> sourceMap) {
        return readAll(keys, sourceMap, identity());
    }

    static <K, V, V1> Map<K, V1> readAll(Iterable<? extends K> keys, Map<K, V> sourceMap, Function<V, V1> mappingFunction) {
        return readAll(keys, sourceMap, LinkedHashMap::new, mappingFunction);
    }

    static <K, V, V1, M extends Map<K, V1>> Map<K, V1> readAll(Iterable<? extends K> keys, Map<K, V> sourceMap, Supplier<M> mapSupplier, Function<V, V1> mappingFunction) {
        return newMap(null, mapSupplier, map -> keys.forEach(key -> ifNotNull(sourceMap.get(key), value -> map.put(key, mappingFunction.apply(value)))));
    }

    static <K, V> Collection<V> removeDuplicates(
            Collection<V> coll,
            Function<? super V, K> keyExtractor) {

        return removeDuplicates(toStream(coll), keyExtractor);
    }

    static <K, V, VC extends Collection<V>> VC removeDuplicates(
            Collection<V> coll,
            Function<? super V, K> keyExtractor,
            Function<Collection<V>, VC> collectionConverter) {

        return removeDuplicates(toStream(coll), keyExtractor, collectionConverter);
    }

    static <K, V, VC extends Collection<V>> VC removeDuplicates(
            Stream<V> stream,
            Function<? super V, K> keyExtractor,
            Function<Collection<V>, VC> collectionConverter) {

        return collectionConverter.apply(removeDuplicates(stream, keyExtractor));
    }

    static <K, V> Collection<V> removeDuplicates(
            Stream<V> stream,
            Function<? super V, K> keyExtractor) {

        return stream
                .collect(toMap(keyExtractor, identity(), (v1, v2) -> v2, LinkedHashMap::new))
                .values();
    }

    static <K, V> LinkedHashMap<K, V> toLinkedHashMap(Map<K, V> map) {
        return map instanceof LinkedHashMap<K, V> lhm ? lhm : new LinkedHashMap<>(map);
    }

    static <T, K> LinkedHashMap<? super K, ? extends T> toLinkedHashMap(Iterable<T> iterable, Function<? super T, ? extends K> keyExtractor) {
        return toLinkedHashMap(iterable, keyExtractor, identity());
    }

    static <T, K, V> LinkedHashMap<K, V> toLinkedHashMap(Iterable<T> iterable, Function<? super T, ? extends K> keyExtractor, Function<? super T, ? extends V> valueExtractor) {
        return toJavaMap(iterable, keyExtractor, valueExtractor, LinkedHashMap::newLinkedHashMap);
    }

    static <T, K, V, M extends Map<K, V>> M toJavaMap(Iterable<T> iterable, Function<? super T, ? extends K> keyExtractor, Function<? super T, ? extends V> valueExtractor, IntFunction<M> mapFactory) {
        final int size = size(iterable);
        return toStream(iterable).collect(toMap(keyExtractor, valueExtractor, (v1, v2) -> v2, () -> mapFactory.apply(size)));
    }

    static <K, V, ID> Map<K, List<V>> mergeMaps(
            Map<K, ? extends List<V>> existingMap,
            Map<K, ? extends List<V>> newMap,
            Function<? super V, ID> idResolver) {

        return mergeMaps(existingMap, newMap, idResolver, ArrayList::new);
    }

    static <K, V, VC extends Collection<V>, ID> Map<K, VC> mergeMaps(
            Map<K, ? extends VC> existingMap,
            Map<K, ? extends VC> newMap,
            Function<? super V, ID> idResolver,
            Function<Collection<V>, VC> collectionConverter) {

        return mergeMaps(existingMap, newMap, (k, coll1, coll2) -> removeDuplicates(concat(coll1, coll2), idResolver, collectionConverter));
    }

    static <K, V, VC extends Collection<V>, V2, VC2 extends Collection<V2>> Map<K, VC> mergeMaps(
            Map<K, ? extends VC> existingMap,
            Map<K, ? extends VC2> newMap,
            Function3<K, ? super VC, ? super Collection<V2>, ? extends VC> mergeFunction,
            Function<Collection<V>, VC> collectionConverter) {

        Function3<K, VC, Collection<V2>, VC> mappingFunction = (k, coll1, coll2) ->
                isNotEmpty(coll1) || isNotEmpty(coll2) ? mergeFunction.apply(k, collectionConverter.apply(coll1), asCollection(coll2)) : collectionConverter.apply(List.of());

        return mergeMaps(existingMap, newMap, mappingFunction);
    }

    static <K, V> Map<K, V> mergeMaps(
            Map<K, ? extends V> existingMap,
            Map<K, ? extends V> newMap) {

        return mergeMaps(existingMap, newMap, (k, v1, v2) -> v2 != null ? v2 : v1);
    }

    static <K, U, V> Map<K, V> mergeMaps(
            Map<K, ? extends V> existingMap,
            Map<K, ? extends U> newMap,
            Function3<K, ? super V, ? super U, ? extends V> mergeFunction) {

        final int size = existingMap.size() + newMap.size();

        final Set<K> allKeys = concat(existingMap.keySet(), newMap.keySet())
                .collect(toCollection(() -> new LinkedHashSet<>(size)));

        return toLinkedHashMap(allKeys, identity(), key -> mergeFunction.apply(key, existingMap.get(key), newMap.get(key)));
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
                            .filter(element -> !idsToSubtract.contains(idResolver.apply(element)))
                            .collect(toCollection(collectionFactory));

                    return isNotEmpty(newColl) ? entry(entry.getKey(), newColl) : null;
                })
                .filter(Objects::nonNull)
                .collect(toMap(Entry::getKey, Entry::getValue, (v1, v2) -> v1, () -> newLinkedHashMap(srcMap.size())));
    }
}
