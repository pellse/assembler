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

import java.util.Optional;
import java.util.function.Function;

import static io.github.pellse.util.ExceptionUtils.sneakyThrow;
import static java.util.Objects.requireNonNull;

@FunctionalInterface
public interface CheckedFunction1<T1, R, E extends Throwable> extends Function<T1, R> {

    R checkedApply(T1 t) throws E;

    @Override
    default R apply(T1 t) {
        try {
            return checkedApply(t);
        } catch (Throwable e) {
            return sneakyThrow(e);
        }
    }

    default <V, E1 extends Throwable> CheckedFunction1<V, R, E1> compose(
            CheckedFunction1<? super V, ? extends T1, E1> before) {
        requireNonNull(before);
        return v -> apply(before.apply(v));
    }

    default <V, E1 extends Throwable> CheckedFunction1<T1, V, E1> andThen(
            CheckedFunction1<? super R, ? extends V, E1> after) {
        requireNonNull(after);
        return t -> after.apply(apply(t));
    }

    default Optional<R> toOptional(T1 t) {
        try {
            return Optional.ofNullable(checkedApply(t));
        } catch (Throwable e) {
            return Optional.empty();
        }
    }

    static <T1, R, E extends Throwable> CheckedFunction1<T1, R, E> of(CheckedFunction1<T1, R, E> function) {
        return function;
    }
}
