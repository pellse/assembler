package io.github.pellse.reactive.assembler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public interface LifeCycleEventBroadcaster extends LifeCycleEventSource {
    void start();

    void stop();

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
}
