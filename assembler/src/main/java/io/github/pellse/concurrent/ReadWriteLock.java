package io.github.pellse.concurrent;

import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

public class ReadWriteLock {

    record LockRequest(MonoSink<Void> sink) {
    }

    private final AtomicBoolean writeLocked = new AtomicBoolean(false);
    private final AtomicInteger readLockCount = new AtomicInteger(0);

    private final Queue<LockRequest> readQueue = new ConcurrentLinkedQueue<>();
    private final Queue<LockRequest> writeQueue = new ConcurrentLinkedQueue<>();


    public Mono<Void> acquireReadLock() {
        return acquireLock(this::canAcquireReadLock, readQueue);
    }

    private boolean canAcquireReadLock() {
        if (!writeLocked.get()) {
            readLockCount.incrementAndGet();
            return true;
        }
        return false;
    }

    public Mono<Void> releaseReadLock() {
        return Mono.fromRunnable(() -> {
            if (readLockCount.decrementAndGet() == 0) {
                processQueues();
            }
        });
    }

    public Mono<Void> acquireWriteLock() {
        return acquireLock(this::canAcquireWriteLock, writeQueue);
    }

    private boolean canAcquireWriteLock() {
        if (!writeLocked.get() && readLockCount.get() == 0) {
            writeLocked.set(true);
            return true;
        }
        return false;
    }

    public Mono<Void> releaseWriteLock() {
        return Mono.fromRunnable(() -> {
            if (writeLocked.get()) {
                writeLocked.set(false);
                processQueues();
            }
        });
    }

    private Mono<Void> acquireLock(BooleanSupplier lockAcquisitionPredicate, Queue<LockRequest> queue) {
        return Mono.create(sink -> {
            if (lockAcquisitionPredicate.getAsBoolean()) {
                sink.success();
            } else {
                queue.offer(new LockRequest(sink));
                processQueues();
            }
        });
    }

    private void processQueues() {
        // Attempt to grant write locks first
        LockRequest nextWriteLock = writeQueue.poll();
        if (nextWriteLock != null) {
            if (canAcquireWriteLock()) {
                nextWriteLock.sink().success();
            } else {
                writeQueue.offer(nextWriteLock);
            }
        } else {
            // If no write locks, grant read locks
            LockRequest nextReadLock;
            while ((nextReadLock = readQueue.poll()) != null) {
                if (canAcquireReadLock()) {
                    nextReadLock.sink().success();
                } else {
                    readQueue.offer(nextReadLock);
                    break;
                }
            }
        }
    }

    public static void main(String[] args) {
        ReadWriteLock lock = new ReadWriteLock();

        // Simulate acquiring a read lock, then upgrading to a write lock
        lock.acquireReadLock()
                .doOnSuccess(unused -> System.out.println("Read lock acquired"))
                .then(lock.acquireWriteLock()
                        .doOnSuccess(unused -> System.out.println("Upgraded to write lock"))
                        .then(lock.releaseWriteLock())
                        .then(lock.releaseReadLock())
                )
                .subscribe();
    }
}
