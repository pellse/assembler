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

import java.util.function.BiConsumer;

import static io.github.pellse.util.ExceptionUtils.sneakyThrow;
import static java.util.Objects.requireNonNull;

@FunctionalInterface
public interface CheckedConsumer2<T, U, E extends Throwable> extends BiConsumer<T, U> {

    void checkedAccept(T t, U u) throws E;

    @Override
    default void accept(T t, U u) {
        try {
            checkedAccept(t, u);
        } catch (Throwable e) {
            sneakyThrow(e);
        }
    }

    @Override
    default CheckedConsumer2<T, U, E> andThen(BiConsumer<? super T, ? super U> after) {
        requireNonNull(after);

        return (l, r) -> {
            accept(l, r);
            after.accept(l, r);
        };
    }

    static <T, U, E extends Throwable> CheckedConsumer2<T, U, E> of(CheckedConsumer2<T, U, E> biConsumer) {
        return biConsumer;
    }
}
