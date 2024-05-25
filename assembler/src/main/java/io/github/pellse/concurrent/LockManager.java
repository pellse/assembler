package io.github.pellse.concurrent;

import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.*;

import static io.github.pellse.util.ObjectUtils.also;
import static reactor.core.publisher.Sinks.EmitResult.FAIL_NON_SERIALIZED;

class LockManager {

    private static final long WRITE_LOCK_BIT = 1L << 63;
    private static final long READ_LOCK_MASK = ~WRITE_LOCK_BIT;

    record Lock(Lock outerLock, Runnable releaseLock) {
        public void release() {
            releaseLock.run();
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

    private final AtomicLong lockState = new AtomicLong();

    private final Queue<LockRequest> readQueue = new ConcurrentLinkedQueue<>();
    private final Queue<LockRequest> writeQueue = new ConcurrentLinkedQueue<>();

    Mono<Lock> acquireReadLock() {
        return acquireLock(null, this::tryAcquireReadLock, this::releaseReadLock, readQueue);
    }

    Mono<Lock> acquireWriteLock() {
        return acquireLock(null, this::tryAcquireWriteLock, this::releaseWriteLock, writeQueue);
    }

    Mono<Lock> convertToWriteLock(Lock lock) {
        return acquireLock(lock, this::tryAcquireWriteLock, this::releaseWriteLock, writeQueue);
    }

    void releaseReadLock() {
        releaseLock(this::doReleaseReadLock);
    }

    void releaseWriteLock() {
        releaseLock(this::doReleaseWriteLock);
    }

    private Mono<Lock> acquireLock(Lock outerLock, Predicate<Lock> tryAcquireLock, Runnable releaseLock, Queue<LockRequest> queue) {
        Lock innerLock = new Lock(outerLock, releaseLock);
        if (tryAcquireLock.test(innerLock)) {
            return Mono.just(innerLock);
        }
        final var sink = Sinks.<Lock>one();
        queue.offer(new LockRequest(innerLock, sink));
        drainQueues();

        return sink.asMono();
    }

    private boolean tryAcquireReadLock(Lock innerLock) {
        return tryAcquireLock(innerLock, (__, currentState) -> (currentState & WRITE_LOCK_BIT) == 0, currentState -> currentState + 1);
    }

    private boolean tryAcquireWriteLock(Lock innerLock) {
        return tryAcquireLock(innerLock, (lock, currentState) -> lock.outerLock() != null || currentState == 0, currentState -> currentState | WRITE_LOCK_BIT);
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

    private void releaseLock(Runnable releaseLockRunnable) {
        releaseLockRunnable.run();
        drainQueues();
    }

    private void doReleaseReadLock() {
        lockState.updateAndGet(currentState -> currentState > 0 ? currentState - 1 : also(0, __ -> System.out.println("Oops! currentState = " + currentState)));
    }

    private void doReleaseWriteLock() {
        lockState.updateAndGet(currentState -> (currentState & WRITE_LOCK_BIT) != 0 ? currentState & READ_LOCK_MASK : currentState); // i.e. noop if calling releaseWriteLock without a corresponding successful tryAcquireWriteLock()
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
