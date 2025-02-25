package io.github.pellse.util.function;

import java.util.function.Consumer;

import static java.lang.Thread.currentThread;

public interface InterruptedFunction3<T1, T2, T3, R> extends CheckedFunction3<T1, T2, T3, R, InterruptedException> {

    @Override
    default Consumer<? super InterruptedException> exceptionHandler() {
        return e -> currentThread().interrupt();
    }
}
