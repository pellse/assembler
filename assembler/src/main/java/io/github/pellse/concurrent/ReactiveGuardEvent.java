package io.github.pellse.concurrent;

public sealed interface ReactiveGuardEvent {

    record LockAcquiredEvent(Lock<?> lock) implements ReactiveGuardEvent {
    }

    record LockUpgradedEvent(Lock<?> lock) implements ReactiveGuardEvent {
    }

    record LockAcquisitionFailedEvent(Throwable error) implements ReactiveGuardEvent {
    }

    record BeforeTaskExecutionEvent(Lock<?> lock) implements ReactiveGuardEvent {
    }

    record AfterTaskExecutionEvent<T>(Lock<?> lock, T value) implements ReactiveGuardEvent {
    }

    record TaskExecutionFailedEvent(Lock<?> lock, Throwable t) implements ReactiveGuardEvent {
    }

    record TaskExecutionTimeoutEvent(Lock<?> lock) implements ReactiveGuardEvent {
    }

    record TaskExecutionCancelledEvent(Lock<?> lock) implements ReactiveGuardEvent {
    }

    record LockReleasedEvent(Lock<?> lock) implements ReactiveGuardEvent {
    }

    record LockReleaseFailedEvent(Lock<?> lock, Throwable t) implements ReactiveGuardEvent {
    }

    record LockReleaseCancelledEvent(Lock<?> lock) implements ReactiveGuardEvent {
    }
}
