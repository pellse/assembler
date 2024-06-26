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

import io.github.pellse.util.concurrent.BoundedQueue;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.*;

import static io.github.pellse.util.concurrent.BoundedQueue.createBoundedQueue;
import static java.lang.Long.MAX_VALUE;
import static reactor.core.publisher.Mono.fromRunnable;
import static reactor.core.publisher.Mono.just;
import static reactor.core.publisher.Sinks.EmitResult.FAIL_NON_SERIALIZED;

class LockManager {

    record Lock(Lock outerLock, Runnable releaseLock) {
        public Mono<?> release() {
            return fromRunnable(releaseLock);
        }
    }

    record LockRequest(Lock lock, Sinks.One<Lock> sink) {
        public boolean emit() {
            Sinks.EmitResult result;
            do {
                result = sink.tryEmitValue(lock());
            } while (result == FAIL_NON_SERIALIZED);
            return result.isSuccess();
        }
    }

    private static final long WRITE_LOCK_MASK = 1L << 63; // 1000000000000000000000000000000000000000000000000000000000000000
    private static final long READ_LOCK_MASK = ~WRITE_LOCK_MASK; // 0111111111111111111111111111111111111111111111111111111111111111

    private final AtomicLong lockState = new AtomicLong();

    private final BoundedQueue<LockRequest> readQueue;
    private final BoundedQueue<LockRequest> writeQueue;

    LockManager() {
        this(MAX_VALUE, MAX_VALUE);
    }

    LockManager(long readQueueCapacity, long writeQueueCapacity) {
        readQueue = createBoundedQueue(readQueueCapacity);
        writeQueue = createBoundedQueue(writeQueueCapacity);
    }

    Mono<Lock> acquireReadLock() {
        return acquireLock(null, this::tryAcquireReadLock, this::releaseReadLock, readQueue);
    }

    Mono<Lock> acquireWriteLock() {
        return toWriteLock(null);
    }

    Mono<Lock> toWriteLock(Lock lock) {
        return acquireLock(lock, this::tryAcquireWriteLock, this::releaseWriteLock, writeQueue);
    }

    void releaseReadLock() {
        releaseLock(this::doReleaseReadLock);
    }

    void releaseWriteLock() {
        releaseLock(this::doReleaseWriteLock);
    }

    private Mono<Lock> acquireLock(Lock outerLock, Predicate<Lock> tryAcquireLock, Runnable releaseLock, BoundedQueue<LockRequest> queue) {
        final var innerLock = new Lock(outerLock, releaseLock);
        if (tryAcquireLock.test(innerLock)) {
            return just(innerLock);
        }

        final var lockRequest = new LockRequest(innerLock, Sinks.one());
        boolean succeeded;
        do {
            succeeded = queue.offer(lockRequest);
            drainQueues();
        } while (!succeeded);

        return lockRequest.sink().asMono();
    }

    private boolean tryAcquireReadLock(Lock innerLock) {
        return tryAcquireLock(innerLock, (__, currentState) -> (currentState & WRITE_LOCK_MASK) == 0, currentState -> currentState + 1);
    }

    private boolean tryAcquireWriteLock(Lock innerLock) {
        return tryAcquireLock(innerLock, (lock, currentState) -> (lock.outerLock() != null && (currentState & WRITE_LOCK_MASK) == 0) || currentState == 0, currentState -> currentState | WRITE_LOCK_MASK);
    }

    private boolean tryAcquireLock(Lock innerLock, BiFunction<Lock, Long, Boolean> currentStatePredicate, LongUnaryOperator currentStateUpdater) {
        long currentState;
        do {
            currentState = lockState.get();
            if (!currentStatePredicate.apply(innerLock, currentState)) {
                return false;
            }
        } while (!lockState.compareAndSet(currentState, currentStateUpdater.applyAsLong(currentState)));
        return true;
    }

    private void releaseLock(Runnable releaseLock) {
        releaseLock.run();
        drainQueues();
    }

    private void doReleaseReadLock() {
        lockState.updateAndGet(currentState -> currentState > 0 ? currentState - 1 : 0);
    }

    private void doReleaseWriteLock() {
        lockState.updateAndGet(currentState -> (currentState & WRITE_LOCK_MASK) != 0 ? currentState & READ_LOCK_MASK : currentState); // i.e. noop if calling releaseWriteLock without a corresponding successful tryAcquireWriteLock()
    }

    private void drainQueues() {
        final var nextWriteLock = writeQueue.poll();
        if (nextWriteLock != null) {
            if (!unlock(nextWriteLock, this::tryAcquireWriteLock, this::doReleaseWriteLock)) {
                writeQueue.offer(nextWriteLock);
            }
        }

        LockRequest nextReadLock;
        while ((nextReadLock = readQueue.poll()) != null) {
            if (!unlock(nextReadLock, this::tryAcquireReadLock, this::doReleaseReadLock)) {
                readQueue.offer(nextReadLock);
                break;
            }
        }
    }

    private boolean unlock(LockRequest lockRequest, Predicate<Lock> tryAcquireLock, Runnable releaseLock) {
        if (tryAcquireLock.test(lockRequest.lock())) {
            if (lockRequest.emit()) {
                return true;
            } else {
                releaseLock.run();
            }
        }
        return false;
    }
}
