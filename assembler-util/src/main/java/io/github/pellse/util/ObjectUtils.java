/*
 * Copyright 2018 Sebastien Pelletier
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

import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public interface ObjectUtils {

    static <T, U> boolean isSafeEqual(T t1, T t2, Function<? super T, ? extends U> propertyExtractor) {
        return isSafeEqual(t1, propertyExtractor, t2, propertyExtractor);
    }

    static <T1, T2, U> boolean isSafeEqual(T1 t1,
                                           Function<? super T1, ? extends U> propertyExtractor1,
                                           T2 t2,
                                           Function<? super T2, ? extends U> propertyExtractor2) {
        return Optional.ofNullable(t1)
                .map(propertyExtractor1)
                .equals(Optional.ofNullable(t2)
                        .map(propertyExtractor2));
    }

    static <T> T also(T value, Consumer<T> codeBlock) {
        codeBlock.accept(value);
        return value;
    }

    static <T> void ifNotNull(T value, Consumer<T> codeBlock) {
        runIf(value, Objects::nonNull, codeBlock);
    }

    static <T> Consumer<T> runIf(Predicate<T> predicate, Runnable runnable) {
        return runIf(predicate, __ -> runnable.run());
    }

    static <T> Consumer<T> runIf(Predicate<T> predicate, Consumer<T> codeBlock) {
        return t -> runIf(t, predicate, codeBlock);
    }

    static <T> void runIf(T value, Predicate<T> predicate, Consumer<T> codeBlock) {
        if (predicate.test(value))
            codeBlock.accept(value);
    }

    static <T> Consumer<T> run(Runnable runnable) {
        return __ -> runnable.run();
    }

    static <T, R> R then(T value, Function<T, R> mappingFunction) {
        return mappingFunction.apply(value);
    }

    static <T> T takeIfNotNull(T value, T defaultValue) {
        return takeIf(value, Objects::nonNull, defaultValue);
    }

    static <T> T takeIf(T value, Predicate<T> predicate, T defaultValue) {
        return predicate.test(value) ? value : defaultValue;
    }
}
