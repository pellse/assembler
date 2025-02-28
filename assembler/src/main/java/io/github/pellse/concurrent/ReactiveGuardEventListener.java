package io.github.pellse.concurrent;

import io.github.pellse.concurrent.CoreLock.WriteLock;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static io.github.pellse.concurrent.ReactiveGuardEvent.*;

public interface ReactiveGuardEventListener<T> {

    default void onLockAcquired(Lock<?> lock) {
    }

    default void onLockUpgraded(WriteLock lock) {
    }

    default void onLockAcquisitionFailed(Throwable t) {
    }

    default void onBeforeTaskExecution(Lock<?> lock) {
    }

    default void onAfterTaskExecution(Lock<?> lock, T value) {
    }

    default void onTaskExecutionFailed(Lock<?> lock, Throwable t) {
    }

    default void onTaskExecutionTimeout(Lock<?> lock) {
    }

    default void onTaskExecutionCancelled(Lock<?> lock) {
    }

    default void onLockReleased(Lock<?> lock) {
    }

    default void onLockReleaseFailed(Lock<?> lock, Throwable t) {
    }

    default void onLockReleaseCancelled(Lock<?> lock) {
    }

    static <T> ReactiveGuardEventListener<T> defaultReactiveGuardListener() {
        return new ReactiveGuardEventListener<>() {};
    }

    static <T> ReactiveGuardEventListener<T> reactiveGuardEventAdapter(BiConsumer<ReactiveGuardEvent, Optional<Lock<?>>> eventConsumer) {
        return reactiveGuardEventAdapter(event -> {
            switch(event) {
                case LockAcquiredEvent lockAcquiredEvent -> eventConsumer.accept(lockAcquiredEvent, Optional.of(lockAcquiredEvent.lock()));
                case LockUpgradedEvent lockUpgradedEvent -> eventConsumer.accept(lockUpgradedEvent, Optional.of(lockUpgradedEvent.lock()));
                case LockAcquisitionFailedEvent lockAcquisitionFailedEvent -> eventConsumer.accept(lockAcquisitionFailedEvent, Optional.empty());
                case AfterTaskExecutionEvent<?> afterTaskExecutionEvent -> eventConsumer.accept(afterTaskExecutionEvent, Optional.of(afterTaskExecutionEvent.lock()));
                case BeforeTaskExecutionEvent beforeTaskExecutionEvent -> eventConsumer.accept(beforeTaskExecutionEvent, Optional.of(beforeTaskExecutionEvent.lock()));
                case TaskExecutionFailedEvent taskExecutionFailedEvent -> eventConsumer.accept(taskExecutionFailedEvent, Optional.of(taskExecutionFailedEvent.lock()));
                case TaskExecutionTimeoutEvent taskExecutionTimeoutEvent -> eventConsumer.accept(taskExecutionTimeoutEvent, Optional.of(taskExecutionTimeoutEvent.lock()));
                case TaskExecutionCancelledEvent taskExecutionCancelledEvent -> eventConsumer.accept(taskExecutionCancelledEvent, Optional.of(taskExecutionCancelledEvent.lock()));
                case LockReleasedEvent lockReleasedEvent -> eventConsumer.accept(lockReleasedEvent, Optional.of(lockReleasedEvent.lock()));
                case LockReleaseFailedEvent lockReleaseFailedEvent -> eventConsumer.accept(lockReleaseFailedEvent, Optional.of(lockReleaseFailedEvent.lock()));
                case LockReleaseCancelledEvent lockReleaseCancelledEvent -> eventConsumer.accept(lockReleaseCancelledEvent, Optional.of(lockReleaseCancelledEvent.lock()));
            }
        });
    }

    static <T> ReactiveGuardEventListener<T> reactiveGuardEventAdapter(Consumer<ReactiveGuardEvent> eventConsumer) {
        return new ReactiveGuardEventListener<>() {

            @Override
            public void onLockAcquired(Lock<?> lock) {
                eventConsumer.accept(new LockAcquiredEvent(lock));
            }

            @Override
            public void onLockUpgraded(WriteLock lock) {
                eventConsumer.accept(new LockUpgradedEvent(lock));
            }

            @Override
            public void onLockAcquisitionFailed(Throwable t) {
                eventConsumer.accept(new LockAcquisitionFailedEvent(t));
            }

            @Override
            public void onBeforeTaskExecution(Lock<?> lock) {
                eventConsumer.accept(new BeforeTaskExecutionEvent(lock));
            }

            @Override
            public void onAfterTaskExecution(Lock<?> lock, T value) {
                eventConsumer.accept(new AfterTaskExecutionEvent<>(lock, value));
            }

            @Override
            public void onTaskExecutionFailed(Lock<?> lock, Throwable t) {
                eventConsumer.accept(new TaskExecutionFailedEvent(lock, t));
            }

            @Override
            public void onTaskExecutionTimeout(Lock<?> lock) {
                eventConsumer.accept(new TaskExecutionTimeoutEvent(lock));
            }

            @Override
            public void onTaskExecutionCancelled(Lock<?> lock) {
                eventConsumer.accept(new TaskExecutionCancelledEvent(lock));
            }

            @Override
            public void onLockReleased(Lock<?> lock) {
                eventConsumer.accept(new LockReleasedEvent(lock));
            }

            @Override
            public void onLockReleaseFailed(Lock<?> lock, Throwable t) {
                eventConsumer.accept(new LockReleaseFailedEvent(lock, t));
            }

            @Override
            public void onLockReleaseCancelled(Lock<?> lock) {
                eventConsumer.accept(new LockReleaseCancelledEvent(lock));
            }
        };
    }
}
