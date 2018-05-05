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

/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.pellse.util.function.checked;

import java.util.function.Consumer;

import static io.github.pellse.util.ExceptionUtils.sneakyThrow;
import static java.util.Objects.requireNonNull;

@FunctionalInterface
public interface CheckedConsumer1<T1, E extends Throwable> extends Consumer<T1> {

    void checkedAccept(T1 t) throws E;

    @Override
    default void accept(T1 t) {
        try {
            checkedAccept(t);
        } catch (Throwable e) {
            sneakyThrow(e);
        }
    }

    default <E1 extends Throwable> CheckedConsumer1<T1, E1> andThen(CheckedConsumer1<? super T1, E1> after) {
        requireNonNull(after);
        return (T1 t) -> {
            accept(t);
            after.accept(t);
        };
    }

    static <T1, E extends Throwable> CheckedConsumer1<T1, E> of(CheckedConsumer1<T1, E> consumer) {
        return consumer;
    }
}
