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

@FunctionalInterface
public interface CheckedRunnable<E extends Throwable> extends Runnable {

    void checkedRun() throws E;

    @Override
    default void run() {
        try {
            checkedRun();
        } catch (Throwable e) {
            sneakyThrow(e);
        }
    }

    static <E1 extends Throwable> CheckedRunnable<E1> of(CheckedRunnable<E1> runnable) {
        return runnable;
    }
}
