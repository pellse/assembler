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

package io.github.pellse.assembler;

import java.util.ArrayList;

public interface LifeCycleEventBroadcaster extends LifeCycleEventSource, LifeCycleEventListener {

    static LifeCycleEventBroadcaster lifeCycleEventBroadcaster() {

        final var listeners = new ArrayList<LifeCycleEventListener>();

        return new LifeCycleEventBroadcaster() {

            @Override
            public void start() {
                    listeners.forEach(LifeCycleEventListener::start);
            }

            @Override
            public void stop() {
                    listeners.forEach(LifeCycleEventListener::stop);
            }

            @Override
            public void addLifeCycleEventListener(LifeCycleEventListener listener) {
                listeners.add(listener);
            }
        };
    }
}
