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

import io.github.pellse.concurrent.CoreLock.ReadLock;
import io.github.pellse.concurrent.CoreLock.WriteLock;
import io.github.pellse.util.function.InterruptedFunction3;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static reactor.core.publisher.Mono.*;

public class StampedLockStrategy implements LockStrategy {

    private final StampedLock stampedLock = new StampedLock();
    private final long timeoutValue;

    public StampedLockStrategy() {
        this(ofSeconds(5));
    }

    public StampedLockStrategy(Duration timeout) {
        this.timeoutValue = timeout.toMillis();
    }

    @Override
    public Mono<? extends Lock<?>> acquireReadLock() {
        return acquireLock(StampedLock::tryReadLock, ReadLock::new, this::releaseReadLock);
    }

    @Override
    public Mono<? extends Lock<?>> acquireWriteLock() {
        return acquireLock(StampedLock::tryWriteLock, WriteLock::new, this::releaseWriteLock);
    }

    @Override
    public Mono<? extends Lock<?>> toWriteLock(Lock<?> lock) {
        return fromSupplier(() -> {
            final var readLock = lock.delegate();
            final var readStamp = readLock.token();

            var writeStamp = stampedLock.tryConvertToWriteLock(readStamp);
            if (writeStamp == 0) {
                stampedLock.unlockRead(readStamp);
                writeStamp = stampedLock.writeLock();
            }
            return new WriteLock(writeStamp, readLock, this::releaseWriteLock);
        });
    }

    @Override
    public void releaseReadLock(ReadLock readLock) {
        releaseLock(readLock);
    }

    @Override
    public void releaseWriteLock(WriteLock writeLock) {
        releaseLock(writeLock);
    }

    private <L extends CoreLock<L>> Mono<? extends Lock<?>> acquireLock(
            InterruptedFunction3<StampedLock, Long, TimeUnit, Long> stampProvider,
            BiFunction<Long, Consumer<L>, Lock<?>> lockProvider,
            Consumer<L> releaseLock) {

        return defer(() -> {
            final var stamp = stampProvider.apply(stampedLock, timeoutValue, MILLISECONDS);
            return stamp != 0 ? just(lockProvider.apply(stamp, releaseLock)) : error(LOCK_ACQUISITION_EXCEPTION);
        });
    }

    private void releaseLock(Lock<?> lock) {
        final var stamp = lock.token();
        if (stampedLock.validate(stamp)) {
            stampedLock.unlock(stamp);
        }
    }
}