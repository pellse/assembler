/*
 * Copyright 2017 Sebastien Pelletier
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

import static io.github.pellse.util.ExceptionUtils.sneakyThrow;
import static java.util.Objects.requireNonNull;

@FunctionalInterface
public interface CheckedPredicate3<T1, T2, T3, E extends Throwable> {

    boolean checkedTest(T1 t1, T2 t2, T3 t3) throws E;

    default boolean test(T1 t1, T2 t2, T3 t3) {
        try {
            return checkedTest(t1, t2, t3);
        } catch (Throwable e) {
            return sneakyThrow(e);
        }
    }

    default CheckedPredicate3<T1, T2, T3, E> negate() {
        return (t1, t2, t3) -> !test(t1, t2, t3);
    }

    default CheckedPredicate3<T1, T2, T3, E> and(CheckedPredicate3<? super T1, ? super T2, ? super T3, E> other) {
        requireNonNull(other);
        return (t1, t2, t3) -> test(t1, t2, t3) && other.test(t1, t2, t3);
    }

    default CheckedPredicate3<T1, T2, T3, E> or(CheckedPredicate3<? super T1, ? super T2, ? super T3, E> other) {
        requireNonNull(other);
        return (t1, t2, t3) -> test(t1, t2, t3) || other.test(t1, t2, t3);
    }

    static <T1, T2, T3, E extends Throwable> CheckedPredicate3<T1, T2, T3, E> of(CheckedPredicate3<T1, T2, T3, E> predicate) {
        return predicate;
    }
}