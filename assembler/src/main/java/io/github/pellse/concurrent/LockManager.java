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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.*;

import static io.github.pellse.concurrent.NoopLock.NOOP_LOCK;
import static java.lang.Thread.onSpinWait;
import static reactor.core.publisher.Mono.just;
import static reactor.core.publisher.Sinks.EmitResult.FAIL_NON_SERIALIZED;
import static reactor.core.publisher.Sinks.EmitResult.OK;

class LockManager {

    private record LockRequest(Lock lock, Sinks.One<Lock> sink) {
        LockRequest(Lock lock) {
            this(lock, Sinks.one());
        }
    }

    private static final long WRITE_LOCK_MASK = 1L << 63; // 1000000000000000000000000000000000000000000000000000000000000000
    private static final long READ_LOCK_MASK = ~WRITE_LOCK_MASK; // 0111111111111111111111111111111111111111111111111111111111111111

    private final AtomicLong lockState = new AtomicLong();

    private final Queue<LockRequest> readQueue = new ConcurrentLinkedQueue<>();
    private final Queue<LockRequest> writeQueue = new ConcurrentLinkedQueue<>();

    Mono<Lock> acquireReadLock() {
        return acquireLock(ReadLock::new, NOOP_LOCK, this::tryAcquireReadLock, this::releaseReadLock, readQueue);
    }

    Mono<Lock> acquireWriteLock() {
        return toWriteLock(NOOP_LOCK);
    }

    Mono<Lock> toWriteLock(Lock lock) {
        return acquireLock(WriteLock::new, lock, this::tryAcquireWriteLock, this::releaseWriteLock, writeQueue);
    }

    void releaseReadLock(Lock innerLock) {
        releaseLock(innerLock, this::doReleaseReadLock);
    }

    void releaseWriteLock(Lock innerLock) {
        releaseLock(innerLock, this::doReleaseWriteLock);
    }

    private Mono<Lock> acquireLock(
            BiFunction<? super Lock, ? super Consumer<Lock>, Lock> lockProvider,
            Lock outerLock,
            Predicate<Lock> tryAcquireLock,
            Consumer<Lock> releaseLock,
            Queue<LockRequest> queue) {

        final var innerLock = lockProvider.apply(outerLock.unwrap(), releaseLock);

        if (tryAcquireLock.test(innerLock)) {
            return just(new WrapperLock(innerLock, this::releaseAndDrain));
        }

        final var lockRequest = new LockRequest(innerLock);
        queue.offer(lockRequest);

        return lockRequest.sink().asMono();
    }

    private Consumer<Lock> releaseAndDrain(Consumer<Lock> releaseLock) {
        return lock -> {
            releaseLock.accept(lock);
            drainQueues();
        };
    }

    private boolean tryAcquireReadLock(Lock innerLock) {
        return tryAcquireLock(
                innerLock,
                (__, currentState) -> (currentState & WRITE_LOCK_MASK) == 0,
                currentState -> currentState + 1);
    }

    private boolean tryAcquireWriteLock(Lock innerLock) {
        BiPredicate<Lock, Long> currentStatePredicate = (lock, currentState) -> switch (lock.outerLock()) {
            case ReadLock __ -> (currentState & WRITE_LOCK_MASK) == 0;
            case WriteLock __ -> (currentState & WRITE_LOCK_MASK) == WRITE_LOCK_MASK;
            case NoopLock __ -> currentState == 0;
            case WrapperLock __ -> throw new IllegalStateException("Unexpected lock state: " + lock);
        };
        return tryAcquireLock(innerLock, currentStatePredicate, currentState -> currentState | WRITE_LOCK_MASK);
    }

    private boolean tryAcquireLock(Lock innerLock, BiPredicate<Lock, Long> currentStatePredicate, LongUnaryOperator currentStateUpdater) {
        long currentState;
        do {
            currentState = lockState.get();
            if (!currentStatePredicate.test(innerLock, currentState)) {
                return false;
            }
        } while (!lockState.compareAndSet(currentState, currentStateUpdater.applyAsLong(currentState)));
        return true;
    }

    private void releaseLock(Lock innerLock, Consumer<Lock> releaseLock) {
        releaseLock.accept(innerLock);
    }

    private void doReleaseReadLock(Lock innerLock) {
        lockState.decrementAndGet();
    }

    private void doReleaseWriteLock(Lock innerLock) {
        if (!(innerLock.outerLock() instanceof WriteLock)) {
            lockState.updateAndGet(currentState -> currentState & READ_LOCK_MASK);
        }
    }

    private void drainQueues() {
        LockRequest writeLockRequest;
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

    private static void drainQueue(Queue<LockRequest> queue, Predicate<Lock> tryAcquireLock, Consumer<Lock> releaseLock) {
        LockRequest lockRequest;
        while ((lockRequest = queue.poll()) != null) {
            while (!unlock(lockRequest, tryAcquireLock, releaseLock)) {
                onSpinWait();
            }
        }
    }

    private static boolean unlock(LockRequest lockRequest, Predicate<Lock> tryAcquireLock, Consumer<Lock> releaseLock) {
        final var innerLock = lockRequest.lock();
        if (tryAcquireLock.test(innerLock)) {
            if (emit(innerLock, lockRequest.sink()) == OK) {
                return true;
            } else {
                releaseLock.accept(innerLock);
            }
        }
        return false;
    }

    private static EmitResult emit(Lock lock, Sinks.One<Lock> sink) {
        EmitResult result;
        while ((result = sink.tryEmitValue(lock)) == FAIL_NON_SERIALIZED) {
            onSpinWait();
        }
        return result;
    }
}
