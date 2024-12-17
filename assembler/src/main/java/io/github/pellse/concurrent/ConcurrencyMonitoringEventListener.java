package io.github.pellse.concurrent;

import java.time.Instant;
import java.util.Map.Entry;

import static java.lang.Long.toBinaryString;
import static java.time.Instant.now;
import static java.util.Arrays.stream;
import static java.util.Map.entry;
import static java.util.stream.Collectors.joining;

@FunctionalInterface
interface ConcurrencyMonitoringEventListener {
    void onLockEvent(ConcurrencyMonitoringEvent concurrencyMonitoringEvent);
}

sealed interface ConcurrencyMonitoringEvent {
    Lock<? extends CoreLock<?>> lock();

    long lockState();

    Instant timestamp();

    String executeOnThread();

    @SafeVarargs
    static String toString(ConcurrencyMonitoringEvent e, Entry<String, String>... extraAttributes) {
        return e.getClass().getSimpleName()
                + "[lock=" + e.lock() + ", lockState=" + toBinaryString(e.lockState()) + ", timestamp=" + e.timestamp() + ", executeOnThread=" + e.executeOnThread() + ", "
                + (extraAttributes.length > 0 ? stream(extraAttributes).map(entry -> entry.getKey() + "=" + entry.getValue()).collect(joining(",")) : "")
                + ']';
    }
}

record LockAcquiredEvent(Lock<?> lock, long lockState, Instant timestamp, String executeOnThread) implements ConcurrencyMonitoringEvent {

    @Override
    public String toString() {
        return ConcurrencyMonitoringEvent.toString(this);
    }
}

record LockReleasedEvent(Lock<?> lock, long lockState, Instant timestamp, String executeOnThread) implements ConcurrencyMonitoringEvent {

    @Override
    public String toString() {
        return ConcurrencyMonitoringEvent.toString(this);
    }
}

record TaskTimedOutEvent(Lock<?> lock, long lockState, Instant timestamp, String executeOnThread, String timedOutThread) implements ConcurrencyMonitoringEvent {
    TaskTimedOutEvent(Lock<?> lock, long lockState, String executeOnThread, String timedOutThread) {
        this(lock, lockState, now(), executeOnThread, timedOutThread);
    }

    @Override
    public String toString() {
        return ConcurrencyMonitoringEvent.toString(this, entry("timedOutThread", timedOutThread));
    }
}