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

package io.github.pellse.reactive.assembler;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

@FunctionalInterface
public interface LifeCycleEventSource {

    static  LifeCycleEventListener concurrentLifeCycleEventListener(LifeCycleEventListener listener) {

        return new LifeCycleEventListener() {

            private final AtomicLong refCount = new AtomicLong();

            @Override
            public void start() {
                if (refCount.getAndIncrement() == 0) {
                    listener.start();
                }
            }

            @Override
            public void stop() {
                if (refCount.decrementAndGet() == 0) {
                    listener.stop();
                }
            }
        };
    }

    static <T, U> LifeCycleEventListener lifeCycleEventAdapter(T eventSource, Function<T, U> start, Consumer<U> stop) {

        return new LifeCycleEventListener() {

            private U stopObj;

            @Override
            public void start() {
                stopObj = start.apply(eventSource);
            }

            @Override
            public void stop() {
                stop.accept(stopObj);
            }
        };
    }

    void addLifeCycleEventListener(LifeCycleEventListener listener);
}