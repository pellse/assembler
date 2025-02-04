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

package io.github.pellse.util;

import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.github.pellse.util.collection.CollectionUtils.isEmpty;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;

public interface ObjectUtils {

    Consumer<?> DO_NOTHING = __ -> {
    };

    @SuppressWarnings("unchecked")
    static <T> Consumer<T> doNothing() {
        return (Consumer<T>) DO_NOTHING;
    }

    static <T, U> boolean isSafeEqual(T t1, T t2, Function<? super T, ? extends U> propertyExtractor) {
        return isSafeEqual(t1, propertyExtractor, t2, propertyExtractor);
    }

    static <T1, T2, U> boolean isSafeEqual(T1 t1,
                                           Function<? super T1, ? extends U> propertyExtractor1,
                                           T2 t2,
                                           Function<? super T2, ? extends U> propertyExtractor2) {

        return ofNullable(t1)
                .map(propertyExtractor1)
                .equals(ofNullable(t2)
                        .map(propertyExtractor2));
    }

    static <T> void ifNotNull(T value, Consumer<T> codeBlock) {
        runIf(value, Objects::nonNull, codeBlock);
    }

    @SafeVarargs
    static <T> T also(T value, Consumer<T>... codeBlocks) {

        if (value == null) {
            return null;
        }

        for (var codeBlock : codeBlocks) {
            codeBlock.accept(value);
        }
        return value;
    }

    @SafeVarargs
    static <T> void run(T value, Consumer<T>... codeBlocks) {
        also(value, codeBlocks);
    }

    static <T> void runIf(T value, Predicate<T> predicate, Consumer<T> codeBlock) {

        if (value != null && predicate.test(value)) {
            codeBlock.accept(value);
        }
    }

    static <T> Consumer<T> run(Runnable runnable) {
        return __ -> runnable.run();
    }

    static <T, R> Function<T, R> get(R value) {
        return __ -> value;
    }

    static <T, R> Function<T, R> get(Supplier<R> supplier) {
        return __ -> supplier.get();
    }

    static <T, R> R then(T value, Function<T, R> mappingFunction) {
        return mappingFunction.apply(value);
    }

    static <T> Predicate<T> or(Predicate<T> predicate1, Predicate<T> predicate2) {
        return nonNull(predicate1).or(nonNull(predicate2));
    }

    static <T> Predicate<T> nonNull(Predicate<T> predicate) {
        return predicate != null ? predicate : t -> true;
    }

    @SafeVarargs
    static String toString(Object obj, Entry<String, ?>... attributes) {
        return ObjectUtils.toString(obj, List.of(attributes));
    }

    static String toString(Object obj, List<Entry<String, ?>> attributes) {
        return obj.getClass().getSimpleName()
                + '[' + (!isEmpty(attributes) ? attributes.stream().map(entry -> entry.getKey() + "=" + entry.getValue()).collect(joining(", ")) : "") + ']';
    }
}
