package io.github.pellse.concurrent;

import io.github.pellse.concurrent.CoreLock.ReadLock;
import io.github.pellse.concurrent.CoreLock.WriteLock;
import reactor.core.publisher.Mono;

public interface LockManager {

    LockAcquisitionException LOCK_ACQUISITION_EXCEPTION = new LockAcquisitionException();

    Mono<? extends Lock<?>> acquireReadLock();

    Mono<? extends Lock<?>> acquireWriteLock();

    Mono<? extends Lock<?>> toWriteLock(Lock<?> lock);

    void releaseReadLock(ReadLock readLock);

    void releaseWriteLock(WriteLock writeLock);

    class LockAcquisitionException extends Exception {
        LockAcquisitionException() {
            super(null, null, true, false);
        }
    }
}
