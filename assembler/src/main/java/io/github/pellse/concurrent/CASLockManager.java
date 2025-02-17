/*
 * Copyright 2024 Sebastien Pelletier
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.pellse.concurrent;

import io.github.pellse.concurrent.CoreLock.NoopLock;
import io.github.pellse.concurrent.CoreLock.ReadLock;
import io.github.pellse.concurrent.CoreLock.WriteLock;
import io.github.pellse.util.function.BiLongPredicate;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.*;

import static io.github.pellse.concurrent.CoreLock.NoopLock.noopLock;
import static io.github.pellse.util.ObjectUtils.doNothing;
import static java.lang.Thread.currentThread;
import static java.time.Duration.ofNanos;
import static java.time.Instant.now;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static reactor.core.publisher.Mono.*;

public class CASLockManager implements LockManager {

    private static final LockAcquisitionException LOCK_ACQUISITION_EXCEPTION = new LockAcquisitionException();

    private static final long WRITE_LOCK_MASK = 1L << 63; // 1000000000000000000000000000000000000000000000000000000000000000
    private static final long READ_LOCK_MASK = ~WRITE_LOCK_MASK; // 0111111111111111111111111111111111111111111111111111111111111111

    private final AtomicLong idCounter = new AtomicLong();
    private final AtomicLong lockState = new AtomicLong();

    private final long maxRetries;
    private final long waitTime;
    private final Consumer<ConcurrencyMonitoringEvent> concurrencyMonitoringEventListener;

    CASLockManager(long maxRetries, Duration waitTime, Consumer<ConcurrencyMonitoringEvent> concurrencyMonitoringEventListener) {
        this.maxRetries = maxRetries;
        this.waitTime = waitTime.toNanos();
        this.concurrencyMonitoringEventListener = concurrencyMonitoringEventListener;
    }

    static Builder lockManagerBuilder() {
        return new Builder();
    }

    static CASLockManager create() {
        return lockManagerBuilder().build();
    }

    @Override
    public Mono<? extends Lock<?>> acquireReadLock() {
        return acquireLock(ReadLock::new, noopLock(), this::tryAcquireReadLock, this::releaseReadLock);
    }

    @Override
    public Mono<? extends Lock<?>> acquireWriteLock() {
        return toWriteLock(noopLock());
    }

    @Override
    public Mono<? extends Lock<?>> toWriteLock(Lock<?> lock) {
        return acquireLock(WriteLock::new, lock, this::tryAcquireWriteLock, this::releaseWriteLock);
    }

    @Override
    public void releaseReadLock(ReadLock innerLock) {
        releaseLock(innerLock, this::doReleaseReadLock);
    }

    @Override
    public void releaseWriteLock(WriteLock innerLock) {
        releaseLock(innerLock, this::doReleaseWriteLock);
    }

    void fireConcurrencyMonitoringEvent(Lock<?> lock, long lockState, ConcurrencyMonitoringEventFactory<?> concurrencyMonitoringEventFactory) {
        fireConcurrencyMonitoringEvent(lock, lockState, 1, concurrencyMonitoringEventFactory);
    }

    void fireConcurrencyMonitoringEvent(Lock<?> lock, long lockState, long nbAttempts, ConcurrencyMonitoringEventFactory<?> concurrencyMonitoringEventFactory) {
        concurrencyMonitoringEventListener.accept(concurrencyMonitoringEventFactory.create(lock, lockState, nbAttempts));
    }

    void fireConcurrencyMonitoringEvent(LongFunction<ConcurrencyMonitoringEvent> concurrencyMonitoringEventProvider) {
        concurrencyMonitoringEventListener.accept(concurrencyMonitoringEventProvider.apply(lockState.get()));
    }

    private <L extends CoreLock<L>> Mono<? extends Lock<?>> acquireLock(
            LockFactory<L> lockFactory,
            Lock<?> outerLock,
            BiPredicate<L, Long> tryAcquireLock,
            Consumer<L> lockReleaser) {

        final var innerLock = lockFactory.create(idCounter.incrementAndGet(), outerLock.unwrap(), lockReleaser);

        long nbAttempts = 0;
        boolean lockAcquired;

        while (!(lockAcquired = tryAcquireLock.test(innerLock, ++nbAttempts)) && nbAttempts <= maxRetries) {
            parkNanos(waitTime);
        }
        if (lockAcquired) {
            return just(innerLock);
        }

        fireConcurrencyMonitoringEvent(innerLock, lockState.get(), nbAttempts, LockAcquisitionFailedEvent::new);
        return error(LOCK_ACQUISITION_EXCEPTION);
    }

    private boolean tryAcquireReadLock(ReadLock innerLock, long nbAttempts) {
        return tryAcquireLock(
                innerLock,
                nbAttempts,
                (__, currentState) -> (currentState & WRITE_LOCK_MASK) == 0,
                currentState -> currentState + 1);
    }

    private boolean tryAcquireWriteLock(WriteLock innerLock, long nbAttempts) {
        BiLongPredicate<WriteLock> currentStatePredicate = (lock, currentState) -> switch (lock.outerLock()) {
            case ReadLock __ -> (currentState & WRITE_LOCK_MASK) == 0; // Nested lock, we try to convert an already acquired read lock to a write lock
            case WriteLock __ -> (currentState & WRITE_LOCK_MASK) == WRITE_LOCK_MASK; // Sanity check, should always be true
            case NoopLock __ -> currentState == 0; // Try to acquire a write lock when no other lock is acquired
        };
        return tryAcquireLock(innerLock, nbAttempts, currentStatePredicate, currentState -> currentState | WRITE_LOCK_MASK);
    }

    private <L extends CoreLock<L>> boolean tryAcquireLock(L innerLock, long nbAttempts,  BiLongPredicate<L> currentStatePredicate, LongUnaryOperator currentStateUpdater) {
        long currentState, newState;
        do {
            currentState = lockState.get();
            if (!currentStatePredicate.test(innerLock, currentState)) {
                return false;
            }
            newState = currentStateUpdater.applyAsLong(currentState);
        } while (!lockState.compareAndSet(currentState, newState));

        fireConcurrencyMonitoringEvent(innerLock, newState, nbAttempts, LockAcquiredEvent::new);
        return true;
    }

    private <L extends CoreLock<L>> void releaseLock(L innerLock, ToLongFunction<L> lockReleaser) {
        fireConcurrencyMonitoringEvent(innerLock, lockReleaser.applyAsLong(innerLock), LockReleasedEvent::new);
    }

    private long doReleaseReadLock(ReadLock innerLock) {
        return lockState.decrementAndGet();
    }

    private long doReleaseWriteLock(WriteLock innerLock) {
        return !(innerLock.outerLock() instanceof WriteLock) ? lockState.updateAndGet(currentState -> currentState & READ_LOCK_MASK) : lockState.get();
    }

    @FunctionalInterface
    interface LockFactory<L extends CoreLock<L>> {
        L create(long id, CoreLock<?> outerLock, Consumer<L> lockReleaser);
    }

    @FunctionalInterface
    interface ConcurrencyMonitoringEventFactory<E extends ConcurrencyMonitoringEvent> {
        E create(Lock<? extends CoreLock<?>> lock, long lockState, long nbAttempts, Instant timestamp, String threadName);

        default E create(Lock<? extends CoreLock<?>> lock, long lockState, long nbAttempts) {
            return create(lock, lockState, nbAttempts, now(), currentThread().getName());
        }
    }

    static class LockAcquisitionException extends Exception {
        LockAcquisitionException() {
            super(null, null, true, false);
        }
    }

    static class Builder {
        private long maxRetries = 1_000;
        private Duration waitTime = ofNanos(1);
        private Consumer<ConcurrencyMonitoringEvent> concurrencyMonitoringEventListener = doNothing();

        public Builder maxRetries(long maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder waitTime(Duration waitTime) {
            this.waitTime = waitTime;
            return this;
        }

        public Builder concurrencyMonitoringEventListener(Consumer<ConcurrencyMonitoringEvent> listener) {
            this.concurrencyMonitoringEventListener = listener;
            return this;
        }

        public CASLockManager build() {
            return new CASLockManager(maxRetries, waitTime, concurrencyMonitoringEventListener);
        }
    }
}