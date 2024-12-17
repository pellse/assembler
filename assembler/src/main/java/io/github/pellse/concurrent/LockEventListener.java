package io.github.pellse.concurrent;

import java.time.Instant;
import java.util.Map.Entry;

import static java.lang.Long.toBinaryString;
import static java.time.Instant.now;
import static java.util.Arrays.stream;
import static java.util.Map.entry;
import static java.util.stream.Collectors.joining;

@FunctionalInterface
interface LockEventListener {
    void onLockEvent(LockEvent lockEvent);
}

sealed interface LockEvent {

    Lock<? extends CoreLock<?>> lock();

    long lockState();

    Instant timestamp();

    String executeOnThread();

    @SafeVarargs
    static String toString(LockEvent e, Entry<String, String>... extraAttributes) {
        return e.getClass().getSimpleName()
                + "[lock=" + e.lock() + ", lockState=" + toBinaryString(e.lockState()) + ", timestamp=" + e.timestamp() + ", executeOnThread=" + e.executeOnThread() + ", "
                + (extraAttributes.length > 0 ? stream(extraAttributes).map(entry -> entry.getKey() + "=" + entry.getValue()).collect(joining(",")) : "")
                + ']';
    }
}

record LockAcquiredEvent(Lock<?> lock, long lockState, Instant timestamp, String executeOnThread) implements LockEvent {

    @Override
    public String toString() {
        return LockEvent.toString(this);
    }
}

record LockReleasedEvent(Lock<?> lock, long lockState, Instant timestamp, String executeOnThread) implements LockEvent {

    @Override
    public String toString() {
        return LockEvent.toString(this);
    }
}

record LockTimedOutEvent(Lock<?> lock, long lockState, Instant timestamp, String executeOnThread, String timedOutThread) implements LockEvent {
    LockTimedOutEvent(Lock<?> lock, long lockState, String executeOnThread, String timedOutThread) {
        this(lock, lockState, now(), executeOnThread, timedOutThread);
    }

    @Override
    public String toString() {
        return LockEvent.toString(this, entry("timedOutThread", timedOutThread));
    }
}