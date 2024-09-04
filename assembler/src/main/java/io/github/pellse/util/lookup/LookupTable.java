package io.github.pellse.util.lookup;

import java.util.*;
import java.util.function.Function;

import static io.github.pellse.util.ObjectUtils.also;
import static java.util.Collections.emptyList;

public interface LookupTable<K, V> {

    void put(K key, V value);

    List<V> get(K key);

    static <K, V> LookupTable<K, V> lookupTable() {

        final Map<K, List<V>> map = new HashMap<>();

        return new LookupTable<>() {

            @Override
            public void put(K key, V value) {
                map.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
            }

            @Override
            public List<V> get(K key) {
                return map.getOrDefault(key, emptyList());
            }
        };
    }

    static <T, K, V> LookupTable<K, V> lookupTableFrom(Iterable<T> elements, Function<? super T, ? extends K> keyMapper, Function<? super T, ? extends V> valueMapper) {
        return also(lookupTable(), lookupTable -> elements.forEach(e -> lookupTable.put(keyMapper.apply(e), valueMapper.apply(e))));
    }
}