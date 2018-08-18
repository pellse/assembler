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

import io.github.pellse.util.function.*;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.github.pellse.util.ExceptionUtils.sneakyThrow;
import static java.util.function.Function.identity;

public interface Unchecked {

    static <R, E extends Throwable> Supplier<R> unchecked(
            CheckedSupplier<R, E> supplier) {
        return unchecked(supplier, identity());
    }

    static <R, E extends Throwable> Supplier<R> unchecked(
            CheckedSupplier<R, E> supplier, Function<? super Throwable, ? extends Throwable> errorConverter) {
        return () -> invoke(supplier::checkedGet, errorConverter);
    }

    static <T1, R, E extends Throwable> Function<T1, R> unchecked(
            CheckedFunction1<T1, R, E> function) {
        return unchecked(function, identity());
    }

    static <T1, R, E extends Throwable> Function<T1, R> unchecked(
            CheckedFunction1<T1, R, E> function, Function<? super Throwable, ? extends Throwable> errorConverter) {
        return t1 -> invoke(() -> function.checkedApply(t1), errorConverter);
    }

    static <T1, T2, R, E extends Throwable> BiFunction<T1, T2, R> unchecked(
            CheckedFunction2<T1, T2, R, E> function) {
        return unchecked(function, identity());
    }

    static <T1, T2, R, E extends Throwable> BiFunction<T1, T2, R> unchecked(
            CheckedFunction2<T1, T2, R, E> function, Function<? super Throwable, ? extends Throwable> errorConverter) {
        return (t1, t2) -> invoke(() -> function.checkedApply(t1, t2), errorConverter);
    }

    static <T1, T2, T3, R, E extends Throwable> Function3<T1, T2, T3, R> unchecked(
            CheckedFunction3<T1, T2, T3, R, E> function) {
        return unchecked(function, identity());
    }

    static <T1, T2, T3, R, E extends Throwable> Function3<T1, T2, T3, R> unchecked(
            CheckedFunction3<T1, T2, T3, R, E> function, Function<? super Throwable, ? extends Throwable> errorConverter) {
        return (t1, t2, t3) -> invoke(() -> function.checkedApply(t1, t2, t3), errorConverter);
    }

    static <T1, T2, T3, T4, R, E extends Throwable> Function4<T1, T2, T3, T4, R> unchecked(
            CheckedFunction4<T1, T2, T3, T4, R, E> function) {
        return unchecked(function, identity());
    }

    static <T1, T2, T3, T4, R, E extends Throwable> Function4<T1, T2, T3, T4, R> unchecked(
            CheckedFunction4<T1, T2, T3, T4, R, E> function, Function<? super Throwable, ? extends Throwable> errorConverter) {
        return (t1, t2, t3, t4) -> invoke(() -> function.checkedApply(t1, t2, t3, t4), errorConverter);
    }

    static <T1, T2, T3, T4, T5, R, E extends Throwable> Function5<T1, T2, T3, T4, T5, R> unchecked(
            CheckedFunction5<T1, T2, T3, T4, T5, R, E> function) {
        return unchecked(function, identity());
    }

    static <T1, T2, T3, T4, T5, R, E extends Throwable> Function5<T1, T2, T3, T4, T5, R> unchecked(
            CheckedFunction5<T1, T2, T3, T4, T5, R, E> function, Function<? super Throwable, ? extends Throwable> errorConverter) {
        return (t1, t2, t3, t4, t5) -> invoke(() -> function.checkedApply(t1, t2, t3, t4, t5), errorConverter);
    }

    static <T1, T2, T3, T4, T5, T6, R, E extends Throwable> Function6<T1, T2, T3, T4, T5, T6, R> unchecked(
            CheckedFunction6<T1, T2, T3, T4, T5, T6, R, E> function) {
        return unchecked(function, identity());
    }

    static <T1, T2, T3, T4, T5, T6, R, E extends Throwable> Function6<T1, T2, T3, T4, T5, T6, R> unchecked(
            CheckedFunction6<T1, T2, T3, T4, T5, T6, R, E> function, Function<? super Throwable, ? extends Throwable> errorConverter) {
        return (t1, t2, t3, t4, t5, t6) -> invoke(() -> function.checkedApply(t1, t2, t3, t4, t5, t6), errorConverter);
    }

    static <T1, T2, T3, T4, T5, T6, T7, R, E extends Throwable> Function7<T1, T2, T3, T4, T5, T6, T7, R> unchecked(
            CheckedFunction7<T1, T2, T3, T4, T5, T6, T7, R, E> function) {
        return unchecked(function, identity());
    }

    static <T1, T2, T3, T4, T5, T6, T7, R, E extends Throwable> Function7<T1, T2, T3, T4, T5, T6, T7, R> unchecked(
            CheckedFunction7<T1, T2, T3, T4, T5, T6, T7, R, E> function, Function<? super Throwable, ? extends Throwable> errorConverter) {
        return (t1, t2, t3, t4, t5, t6, t7) -> invoke(() -> function.checkedApply(t1, t2, t3, t4, t5, t6, t7), errorConverter);
    }

    static <T1, T2, T3, T4, T5, T6, T7, T8, R, E extends Throwable> Function8<T1, T2, T3, T4, T5, T6, T7, T8, R> unchecked(
            CheckedFunction8<T1, T2, T3, T4, T5, T6, T7, T8, R, E> function) {
        return unchecked(function, identity());
    }

    static <T1, T2, T3, T4, T5, T6, T7, T8, R, E extends Throwable> Function8<T1, T2, T3, T4, T5, T6, T7, T8, R> unchecked(
            CheckedFunction8<T1, T2, T3, T4, T5, T6, T7, T8, R, E> function, Function<? super Throwable, ? extends Throwable> errorConverter) {
        return (t1, t2, t3, t4, t5, t6, t7, t8) -> invoke(() -> function.checkedApply(t1, t2, t3, t4, t5, t6, t7, t8), errorConverter);
    }

    private static <R> R invoke(CheckedSupplier<R, Throwable> supplier, Function<? super Throwable, ? extends Throwable> errorConverter) {
        try {
            return supplier.checkedGet();
        } catch (Throwable e) {
            return sneakyThrow(errorConverter.apply(e));
        }
    }
}
