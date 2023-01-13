package io.github.pellse.reactive.assembler;

import java.util.function.Consumer;
import java.util.function.Function;

public interface LifeCycleEventSource {

    interface LifeCycleEventListener {
        void start();

        void stop();
    }

    void addLifeCycleEventListener(LifeCycleEventListener listener);

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
}