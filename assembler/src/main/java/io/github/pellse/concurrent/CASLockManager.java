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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.*;

import static io.github.pellse.concurrent.CoreLock.NoopLock.noopLock;
import static java.time.Duration.ofNanos;
import static reactor.core.publisher.Mono.*;
import static reactor.util.retry.Retry.fixedDelay;

public class CASLockManager implements LockManager {

    private static final long WRITE_LOCK_MASK = 1L << 63; // 1000000000000000000000000000000000000000000000000000000000000000
    private static final long READ_LOCK_MASK = ~WRITE_LOCK_MASK; // 0111111111111111111111111111111111111111111111111111111111111111

    private final AtomicLong idCounter = new AtomicLong();
    private final AtomicLong lockState = new AtomicLong();

    private final long maxRetries;
    private final Duration waitTime;

    public CASLockManager() {
        this(1_000, ofNanos(1));
    }

    public CASLockManager(long maxRetries, Duration waitTime) {
        this.maxRetries = maxRetries;
        this.waitTime = waitTime;
    }

    @Override
    public Mono<? extends Lock<?>> acquireReadLock() {
        return acquireLock(ReadLock::new, this::tryAcquireReadLock, this::releaseReadLock);
    }

    @Override
    public Mono<? extends Lock<?>> acquireWriteLock() {
        return acquireLock(WriteLock::new, this::tryAcquireWriteLock, this::releaseWriteLock);
    }

    @Override
    public Mono<? extends Lock<?>> toWriteLock(Lock<?> lock) {
        return acquireLock(WriteLock::new, lock, this::tryAcquireWriteLock, this::releaseWriteLock);
    }

    @Override
    public void releaseReadLock(ReadLock readLock) {
        releaseLock(readLock, this::doReleaseReadLock);
    }

    @Override
    public void releaseWriteLock(WriteLock writeLock) {
        releaseLock(writeLock, this::doReleaseWriteLock);
    }

    private <L extends CoreLock<L>> Mono<? extends Lock<?>> acquireLock(
            LockFactory<L> lockFactory,
            Predicate<L> tryAcquireLock,
            Consumer<L> lockReleaser) {

        return acquireLock(lockFactory, noopLock(), tryAcquireLock, lockReleaser);
    }

    private <L extends CoreLock<L>> Mono<? extends Lock<?>> acquireLock(
            LockFactory<L> lockFactory,
            Lock<?> outerLock,
            Predicate<L> tryAcquireLock,
            Consumer<L> lockReleaser) {

        return defer(() -> {
            final var innerLock = lockFactory.create(idCounter.incrementAndGet(), outerLock.unwrap(), lockReleaser);
            return tryAcquireLock.test(innerLock) ? just(innerLock) : error(LOCK_ACQUISITION_EXCEPTION);
        })
                .retryWhen(fixedDelay(maxRetries, waitTime).filter(LockAcquisitionException.class::isInstance));
    }

    private boolean tryAcquireReadLock(ReadLock innerLock) {
        return tryAcquireLock(
                innerLock,
                (__, currentState) -> (currentState & WRITE_LOCK_MASK) == 0,
                currentState -> currentState + 1);
    }

    private boolean tryAcquireWriteLock(WriteLock innerLock) {
        BiLongPredicate<WriteLock> currentStatePredicate = (lock, currentState) -> switch (lock.outerLock()) {
            case ReadLock __ -> (currentState & WRITE_LOCK_MASK) == 0; // Nested lock, we try to convert an already acquired read lock to a write lock
            case WriteLock __ -> (currentState & WRITE_LOCK_MASK) == WRITE_LOCK_MASK; // Sanity check, should always be true
            case NoopLock __ -> currentState == 0; // Try to acquire a write lock when no other lock is acquired
        };
        return tryAcquireLock(innerLock, currentStatePredicate, currentState -> currentState | WRITE_LOCK_MASK);
    }

    private <L extends CoreLock<L>> boolean tryAcquireLock(L innerLock, BiLongPredicate<L> currentStatePredicate, LongUnaryOperator currentStateUpdater) {
        long currentState, newState;
        do {
            currentState = lockState.get();
            if (!currentStatePredicate.test(innerLock, currentState)) {
                return false;
            }
            newState = currentStateUpdater.applyAsLong(currentState);
        } while (!lockState.compareAndSet(currentState, newState));

        return true;
    }

    private <L extends CoreLock<L>> void releaseLock(L innerLock, ToLongFunction<L> lockReleaser) {
        lockReleaser.applyAsLong(innerLock);
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
}