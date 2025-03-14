package io.github.pellse.util.lock;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public interface LockSupplier {

    static <T> Supplier<T> withLock(Supplier<T> supplier, AtomicBoolean lock) {
        return () -> {
            while (true) {
                if (lock.compareAndSet(false, true)) {
                    try {
                        return supplier.get();
                    } finally {
                        lock.set(false);
                    }
                }
            }
        };
    }

    static <T> T executeWithLock(Supplier<T> supplier, AtomicBoolean lock) {
        return withLock(supplier, lock).get();
    }
}