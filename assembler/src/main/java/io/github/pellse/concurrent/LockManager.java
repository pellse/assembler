package io.github.pellse.concurrent;

import io.github.pellse.concurrent.CoreLock.ReadLock;
import io.github.pellse.concurrent.CoreLock.WriteLock;
import reactor.core.publisher.Mono;

public interface LockManager {

    Mono<? extends Lock<?>> acquireReadLock();

    Mono<? extends Lock<?>> acquireWriteLock();

    Mono<? extends Lock<?>> toWriteLock(Lock<?> lock);

    void releaseReadLock(ReadLock innerLock);

    void releaseWriteLock(WriteLock innerLock);
}
