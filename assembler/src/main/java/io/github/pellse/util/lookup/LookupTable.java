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