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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public interface LifeCycleEventBroadcaster extends LifeCycleEventSource {
    static LifeCycleEventBroadcaster lifeCycleEventBroadcaster() {

        return new LifeCycleEventBroadcaster() {

            private final List<LifeCycleEventListener> listeners = new ArrayList<>();
            private final AtomicBoolean isStarted = new AtomicBoolean();

            @Override
            public void start() {
                if (isStarted.compareAndSet(false, true)) {
                    listeners.forEach(LifeCycleEventListener::start);
                }
            }

            @Override
            public void stop() {
                if (isStarted.get()) {
                    listeners.forEach(LifeCycleEventListener::stop);
                }
            }

            @Override
            public void addLifeCycleEventListener(LifeCycleEventListener listener) {
                listeners.add(listener);
            }
        };
    }

    void start();

    void stop();
}
