package io.github.pellse.concurrent;

import io.github.pellse.util.ObjectUtils;

import java.time.Instant;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static java.lang.Long.toBinaryString;
import static java.util.Arrays.stream;
import static java.util.Map.entry;
import static java.util.stream.Stream.concat;

public sealed interface ConcurrencyMonitoringEvent {
    Lock<? extends CoreLock<?>> lock();

    long lockState();

    long nbAttempts();

    Instant timestamp();

    String executeOnThread();

    default String log() {
        return ConcurrencyMonitoringEvent.log(this);
    }

    @SafeVarargs
    static String log(ConcurrencyMonitoringEvent e, Entry<String, Object>... extraAttributes) {
        return ObjectUtils.toString(e, concat(
                Stream.of(
                        entry("lock", e.lock().log()),
                        entry("lockState", toBinaryString(e.lockState())),
                        entry("nbAttempts", e.nbAttempts()),
                        entry("timestamp", e.timestamp()),
                        entry("executeOnThread", e.executeOnThread())
                ), stream(extraAttributes)
        ).toList());
    }
}

record LockAcquiredEvent(Lock<?> lock, long lockState, long nbAttempts, Instant timestamp, String executeOnThread) implements ConcurrencyMonitoringEvent {
}

record LockReleasedEvent(Lock<?> lock, long lockState, long nbAttempts, Instant timestamp, String executeOnThread) implements ConcurrencyMonitoringEvent {
}

record LockAcquisitionFailedEvent(Lock<?> lock, long lockState, long nbAttempts, Instant timestamp, String executeOnThread) implements ConcurrencyMonitoringEvent {
}