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

package io.github.pellse.util.function;

import java.util.function.Consumer;

import static io.github.pellse.util.ObjectUtils.doNothing;
import static io.github.pellse.util.ObjectUtils.sneakyThrow;

@FunctionalInterface
public interface CheckedFunction3<T1, T2, T3, R, E extends Throwable> extends Function3<T1, T2, T3, R> {

    R checkedApply(T1 t1, T2 t2, T3 t3) throws E;

    @Override
    default R apply(T1 t1, T2 t2, T3 t3) {
        return applyWithHandler(t1, t2, t3, exceptionHandler());
    }

    @SuppressWarnings("unchecked")
    default R applyWithHandler(T1 t1, T2 t2, T3 t3, Consumer<? super E> exceptionHandler) {
        try {
            return checkedApply(t1, t2, t3);
        } catch (Throwable e) {
            exceptionHandler.accept((E) e);
            return sneakyThrow(e);
        }
    }

    default Consumer<? super E> exceptionHandler() {
        return doNothing();
    }
}
