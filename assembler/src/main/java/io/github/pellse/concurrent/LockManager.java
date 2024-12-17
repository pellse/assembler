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

import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmitResult;

import java.time.Instant;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.*;

import static io.github.pellse.concurrent.NoopLock.noopLock;
import static java.lang.Thread.currentThread;
import static java.lang.Thread.onSpinWait;
import static java.time.Instant.now;
import static reactor.core.publisher.Mono.just;
import static reactor.core.publisher.Sinks.EmitResult.FAIL_NON_SERIALIZED;

class LockManager {

    @FunctionalInterface
    interface LockReleaser<L extends CoreLock<L>> {
        long release(L lock);
    }

    @FunctionalInterface
    interface ConcurrencyMonitoringEventFactory<E extends ConcurrencyMonitoringEvent> {
        E create(Lock<? extends CoreLock<?>> lock, long lockState, Instant timestamp, String threadName);

        default E create(Lock<? extends CoreLock<?>> lock, long lockState) {
            return create(lock, lockState, now(), currentThread().getName());
        }
    }

    @FunctionalInterface
    interface ConcurrencyMonitoringEventProvider<E extends ConcurrencyMonitoringEvent> {
        E create(long lockState);
    }

    private record LockRequest<L extends CoreLock<L>>(L lock, Sinks.One<L> sink) {
        LockRequest(L lock) {
            this(lock, Sinks.one());
        }
    }

    private static final long WRITE_LOCK_MASK = 1L << 63; // 1000000000000000000000000000000000000000000000000000000000000000
    private static final long READ_LOCK_MASK = ~WRITE_LOCK_MASK; // 0111111111111111111111111111111111111111111111111111111111111111

    private final AtomicLong idCounter = new AtomicLong();

    private final AtomicLong lockState = new AtomicLong();

    private final Queue<LockRequest<ReadLock>> readQueue = new ConcurrentLinkedQueue<>();
    private final Queue<LockRequest<WriteLock>> writeQueue = new ConcurrentLinkedQueue<>();

    private final ConcurrencyMonitoringEventListener concurrencyMonitoringEventListener;

    LockManager() {
        this(__ -> {
        });
    }

    LockManager(ConcurrencyMonitoringEventListener concurrencyMonitoringEventListener) {
        this.concurrencyMonitoringEventListener = concurrencyMonitoringEventListener;
    }

    void fireConcurrencyMonitoringEvent(Lock<?> lock, long lockState, ConcurrencyMonitoringEventFactory<?> concurrencyMonitoringEventFactory) {
        concurrencyMonitoringEventListener.onLockEvent(concurrencyMonitoringEventFactory.create(lock, lockState));
    }

    void fireConcurrencyMonitoringEvent(ConcurrencyMonitoringEventProvider<?> concurrencyMonitoringEventFactory) {
        concurrencyMonitoringEventListener.onLockEvent(concurrencyMonitoringEventFactory.create(lockState.get()));
    }

    Mono<? extends Lock<?>> acquireReadLock() {
        return acquireLock(ReadLock::new, noopLock(), this::tryAcquireReadLock, this::releaseReadLock, readQueue);
    }

    Mono<? extends Lock<?>> acquireWriteLock() {
        return toWriteLock(noopLock());
    }

    Mono<? extends Lock<?>> toWriteLock(Lock<?> lock) {
        return acquireLock(WriteLock::new, lock, this::tryAcquireWriteLock, this::releaseWriteLock, writeQueue);
    }

    void releaseReadLock(ReadLock innerLock) {
        releaseLock(innerLock, this::doReleaseReadLock);
    }

    void releaseWriteLock(WriteLock innerLock) {
        releaseLock(innerLock, this::doReleaseWriteLock);
    }

    private <L extends CoreLock<L>> Mono<? extends Lock<?>> acquireLock(
            LockFactory<L> lockFactory,
            Lock<?> outerLock,
            Predicate<L> tryAcquireLock,
            Consumer<L> lockReleaser,
            Queue<LockRequest<L>> queue) {

        final var innerLock = lockFactory.create(idCounter.incrementAndGet(), outerLock.unwrap(), lockReleaser);

        if (tryAcquireLock.test(innerLock)) {
            return just(new WrapperLock<>(innerLock, this::releaseAndDrain));
        }

        final var lockRequest = new LockRequest<>(innerLock);
        queue.offer(lockRequest);

        return lockRequest.sink().asMono();
    }

    private <L extends CoreLock<L>> Consumer<L> releaseAndDrain(Consumer<L> lockReleaser) {
        return lock -> {
            lockReleaser.accept(lock);
            drainQueues();
        };
    }

    private boolean tryAcquireReadLock(ReadLock innerLock) {
        return tryAcquireLock(
                innerLock,
                (__, currentState) -> (currentState & WRITE_LOCK_MASK) == 0,
                currentState -> currentState + 1);
    }

    private boolean tryAcquireWriteLock(WriteLock innerLock) {
        BiPredicate<WriteLock, Long> currentStatePredicate = (lock, currentState) -> switch (lock.outerLock()) {
            case ReadLock __ -> (currentState & WRITE_LOCK_MASK) == 0;
            case WriteLock __ -> (currentState & WRITE_LOCK_MASK) == WRITE_LOCK_MASK;
            case NoopLock __ -> currentState == 0;
        };
        return tryAcquireLock(innerLock, currentStatePredicate, currentState -> currentState | WRITE_LOCK_MASK);
    }

    private <L extends CoreLock<L>> boolean tryAcquireLock(L innerLock, BiPredicate<L, Long> currentStatePredicate, LongUnaryOperator currentStateUpdater) {
        long currentState, newState;
        do {
            currentState = lockState.get();
            if (!currentStatePredicate.test(innerLock, currentState)) {
                return false;
            }
            newState = currentStateUpdater.applyAsLong(currentState);
        } while (!lockState.compareAndSet(currentState, newState));

        fireConcurrencyMonitoringEvent(innerLock, newState, LockAcquiredEvent::new);
        return true;
    }

    private <L extends CoreLock<L>> void releaseLock(L innerLock, LockReleaser<L> lockReleaser) {
        fireConcurrencyMonitoringEvent(innerLock, lockReleaser.release(innerLock), LockReleasedEvent::new);
    }

    private long doReleaseReadLock(ReadLock innerLock) {
        return lockState.decrementAndGet();
    }

    private long doReleaseWriteLock(WriteLock innerLock) {
        return !(innerLock.outerLock() instanceof WriteLock) ? lockState.updateAndGet(currentState -> currentState & READ_LOCK_MASK) : lockState.get();
    }

    private void drainQueues() {
        LockRequest<WriteLock> writeLockRequest;
        while ((writeLockRequest = writeQueue.poll()) != null) {
            if (!unlock(writeLockRequest, this::tryAcquireWriteLock, this::doReleaseWriteLock)) {
                onSpinWait();
                drainReadQueue();
            }
        }
        drainReadQueue();
    }

    private void drainReadQueue() {
        drainQueue(readQueue, this::tryAcquireReadLock, this::doReleaseReadLock);
    }

    private static <L extends CoreLock<L>> void drainQueue(Queue<LockRequest<L>> queue, Predicate<L> tryAcquireLock, Consumer<L> lockReleaser) {
        LockRequest<L> lockRequest;
        while ((lockRequest = queue.poll()) != null) {
            while (!unlock(lockRequest, tryAcquireLock, lockReleaser)) {
                onSpinWait();
            }
        }
    }

    private static <L extends CoreLock<L>> boolean unlock(LockRequest<L> lockRequest, Predicate<L> tryAcquireLock, Consumer<L> lockReleaser) {
        final var innerLock = lockRequest.lock();
        if (tryAcquireLock.test(innerLock)) {
            if (emit(innerLock, lockRequest.sink()).isSuccess()) {
                return true;
            } else {
                lockReleaser.accept(innerLock);
            }
        }
        return false;
    }

    private static <L extends CoreLock<L>> EmitResult emit(L lock, Sinks.One<L> sink) {
        EmitResult result;
        while ((result = sink.tryEmitValue(lock)) == FAIL_NON_SERIALIZED) {
            onSpinWait();
        }
        return result;
    }
}
