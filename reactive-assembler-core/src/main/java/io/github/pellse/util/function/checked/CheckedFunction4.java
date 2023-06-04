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

package io.github.pellse.util.function.checked;

import io.github.pellse.util.function.Function4;

import static io.github.pellse.util.ExceptionUtils.sneakyThrow;
import static java.util.Objects.requireNonNull;

@FunctionalInterface
public interface CheckedFunction4<T1, T2, T3, T4, R, E extends Throwable> extends Function4<T1, T2, T3, T4, R> {

    R checkedApply(T1 t1, T2 t2, T3 t3, T4 t4) throws E;

    default R apply(T1 t1, T2 t2, T3 t3, T4 t4) {
        try {
            return checkedApply(t1, t2, t3, t4);
        } catch (Throwable e) {
            return sneakyThrow(e);
        }
    }

    default <V, E1 extends Throwable> CheckedFunction4<T1, T2, T3, T4, V, E1> andThen(
            CheckedFunction1<? super R, ? extends V, E1> after) {
        requireNonNull(after);
        return (t1, t2, t3, t4) -> after.apply(apply(t1, t2, t3, t4));
    }

    static <T1, T2, T3, T4, R, E extends Throwable> CheckedFunction4<T1, T2, T3, T4, R, E> of(
            CheckedFunction4<T1, T2, T3, T4, R, E> function) {
        return function;
    }
}
