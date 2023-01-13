package io.github.pellse.reactive.assembler;

import java.util.ArrayList;
import java.util.List;

public interface LifeCycleEventBroadcaster extends LifeCycleEventSource {
    void start();

    void stop();

    static LifeCycleEventBroadcaster lifeCycleEventBroadcaster() {

        return new LifeCycleEventBroadcaster() {

            private final List<LifeCycleEventListener> listeners = new ArrayList<>();

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
