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

package io.github.pellse.util.function.checked;

import java.util.function.BiPredicate;

import static io.github.pellse.util.ExceptionUtils.sneakyThrow;
import static java.util.Objects.requireNonNull;

@FunctionalInterface
public interface CheckedPredicate2<T1, T2, E extends Throwable> extends BiPredicate<T1, T2> {

    boolean checkedTest(T1 t1, T2 t2) throws E;

    @Override
    default boolean test(T1 t1, T2 t2) {
        try {
            return checkedTest(t1, t2);
        } catch (Throwable e) {
            return sneakyThrow(e);
        }
    }

    @Override
    default CheckedPredicate2<T1, T2, E> negate() {
        return (t1, t2) -> !test(t1, t2);
    }

    default CheckedPredicate2<T1, T2, E> and(CheckedPredicate2<? super T1, ? super T2, E> other) {
        requireNonNull(other);
        return (t1, t2) -> test(t1, t2) && other.test(t1, t2);
    }

    default CheckedPredicate2<T1, T2, E> or(CheckedPredicate2<? super T1, ? super T2, E> other) {
        requireNonNull(other);
        return (t1, t2) -> test(t1, t2) || other.test(t1, t2);
    }

    static <T1, T2, E extends Throwable> CheckedPredicate2<T1, T2, E> of(CheckedPredicate2<T1, T2, E> predicate) {
        return predicate;
    }

    static <T1, T2> BiPredicate<T1, T2> not(BiPredicate<T1, T2> predicate) {
        return (t1, t2) -> !predicate.test(t1, t2);
    }
}