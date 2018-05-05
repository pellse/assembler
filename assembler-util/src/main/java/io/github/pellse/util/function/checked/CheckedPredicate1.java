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

/**
 * @author Sebastien Pelletier
 *
 */

import static io.github.pellse.util.ExceptionUtils.sneakyThrow;
import static java.util.Objects.requireNonNull;
import static java.util.stream.StreamSupport.stream;

        import java.util.function.Predicate;

@FunctionalInterface
public interface CheckedPredicate1<T, E extends Throwable> extends Predicate<T> {

    boolean checkedTest(T t) throws E;

    @Override
    default boolean test(T t) {
        try {
            return checkedTest(t);
        } catch (Throwable e) {
            return sneakyThrow(e);
        }
    }

    @Override
    default CheckedPredicate1<T, E> negate() {
        return t -> !test(t);
    }

    default CheckedPredicate1<T, E> and(CheckedPredicate1<? super T, E> other) {
        requireNonNull(other);
        return t -> test(t) && other.test(t);
    }

    default CheckedPredicate1<T, E> or(CheckedPredicate1<? super T, E> other) {
        requireNonNull(other);
        return t -> test(t) || other.test(t);
    }

    static <T, E extends Throwable> CheckedPredicate1<T, E> of(CheckedPredicate1<T, E> predicate) {
        return predicate;
    }

    static <T> Predicate<T> not(Predicate<T> predicate) {
        return predicate.negate();
    }
}
