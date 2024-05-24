package io.github.pellse.concurrent;

import reactor.core.publisher.Mono;
import java.util.function.Function;

import static reactor.core.publisher.Mono.*;

public interface ReentrantExecutor {

    default <T> Mono<T> withReadLock(Mono<T> mono) {
        return withReadLock(__ -> mono);
    }

    <T> Mono<T> withReadLock(Function<WriteLockExecutor, Mono<T>> monoProvider);

    <T> Mono<T> withWriteLock(Mono<T> mono);

    @FunctionalInterface
    interface WriteLockExecutor {
        <U> Mono<U> withWriteLock(Mono<U> mono);
    }

    static ReentrantExecutor create() {

        final var lockManager = new LockManager();

        return new ReentrantExecutor() {

            @Override
            public <T> Mono<T> withReadLock(Function<WriteLockExecutor, Mono<T>> monoProvider) {
                return with(monoProvider, LockManager::acquireReadLock);
            }

            @Override
            public <T> Mono<T> withWriteLock(Mono<T> mono) {
                return with(__ -> mono, LockManager::acquireWriteLock);
            }

            private <T> Mono<T> with(Function<WriteLockExecutor, Mono<T>> monoProvider, Function<LockManager, Mono<LockManager.Lock>> acquireLockStrategy) {
                return usingWhen(
                        acquireLockStrategy.apply(lockManager),
                        lock -> monoProvider.apply(new WriteLockExecutor() {
                                    @Override
                                    public <U> Mono<U> withWriteLock(Mono<U> mono) {
                                        return usingWhen(
                                                lockManager.convertToWriteLock(lock),
                                                writeLock -> mono,
                                                writeLock -> Mono.fromRunnable(writeLock::release)
                                        );
                                    }
                                }),
                        lock -> Mono.fromRunnable(lock::release)
                );
            }
        };
    }
}

//import reactor.core.publisher.Mono;
//import reactor.core.publisher.Sinks;
//import reactor.core.scheduler.Scheduler;
//
//import java.util.concurrent.ConcurrentLinkedQueue;
//import java.util.concurrent.atomic.AtomicBoolean;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.concurrent.atomic.AtomicReference;
//import java.util.function.Consumer;
//import java.util.function.Function;
//import java.util.function.Predicate;
//
//import static java.time.Duration.ofSeconds;
//import static reactor.core.scheduler.Schedulers.immediate;
//
//public interface ReentrantExecutor {
//
//    default <T> Mono<T> withReadLock(Mono<T> mono) {
//        return withReadLock(__ -> mono);
//    }
//
//    <T> Mono<T> withReadLock(Function<WriteLockExecutor, Mono<T>> monoProvider);
//
//    <T> Mono<T> withWriteLock(Mono<T> mono);
//
//    @FunctionalInterface
//    interface WriteLockExecutor {
//        <U> Mono<U> withWriteLock2(Mono<U> mono);
//    }
//
//    record LockId(long lockAcquiredCount, LockId outerLock) {
//        public LockId() {
//            this(0, null);
//        }
//
//        public LockId newLockId() {
//            return new LockId(lockAcquiredCount() + 1, this);
//        }
//    }
//
//    record LockRequest(LockId lockId, Sinks.Empty<Void> sink) {
//    }
//
//    static ReentrantExecutor create() {
//        return create(immediate());
//    }
//
//    static ReentrantExecutor create(Scheduler scheduler) {
//        return create(scheduler, scheduler);
//    }
//
//    static ReentrantExecutor create(Scheduler readScheduler, Scheduler writeScheduler) {
//
//        final var readLockCount = new AtomicInteger(0);
//        final var writeLockCount = new AtomicInteger(0);
//
//        final var readWaiters = new ConcurrentLinkedQueue<LockRequest>();
//        final var writeWaiters = new ConcurrentLinkedQueue<LockRequest>();
//
////        final var currentWriter = new AtomicReference<LockId>(null);
//        final var isProcessing = new AtomicBoolean();
//        final var WIP = new AtomicInteger();
//
//        return new ReentrantExecutor() {
//
//            @Override
//            public <T> Mono<T> withReadLock(Function<WriteLockExecutor, Mono<T>> monoProvider) {
//                return withLock(null, monoProvider, this::tryAcquireReadLock, this::releaseReadLock, readWaiters);
//            }
//
//            @Override
//            public <T> Mono<T> withWriteLock(Mono<T> mono) {
//                return withWriteLock(null, mono);
//            }
//
//            private <T> Mono<T> withWriteLock(LockId lockId, Mono<T> mono) {
//                return withLock(lockId, __ -> mono, this::tryAcquireWriteLock, this::releaseWriteLock, writeWaiters);
//            }
//
//            private <T> Mono<T> withLock(
//                    LockId lockId,
//                    Function<WriteLockExecutor, Mono<T>> monoProvider,
//                    Predicate<LockId> tryAcquireLock,
//                    Consumer<LockId> releaseLock,
//                    ConcurrentLinkedQueue<LockRequest> queue) {
//
//                final var isFirstInvocation = lockId == null;
//                final var actualLockId = isFirstInvocation ? new LockId() : lockId;
//
//                return Mono.defer(() -> {
//                    if (tryAcquireLock.test(actualLockId)) {
//                        return Mono.using(
//                                actualLockId::newLockId,
//                                newLockId -> monoProvider.apply(new WriteLockExecutor() {
//                                    @Override
//                                    public <U> Mono<U> withWriteLock2(Mono<U> mono) {
//                                        return withWriteLock(newLockId, mono);
//                                    }
//                                }),
//                                __ -> {
//                                    releaseLock.accept(actualLockId);
////                                    if (isFirstInvocation) {
//                                        drainWaiters();
////                                    }
//                                });
//                    } else {
//                        Sinks.Empty<Void> sink = Sinks.empty();
//                        queue.offer(new LockRequest(actualLockId, sink));
//
//                        return sink.asMono()
//                                .timeout(ofSeconds(30), Mono.fromRunnable(() -> System.out.println("Timeout")))
//                                .then(withLock(actualLockId, monoProvider, tryAcquireLock, releaseLock, queue));
//                    }
//                }).subscribeOn(writeScheduler);
//            }
//
//            private boolean tryAcquireReadLock(LockId lockId) {
//
////                final var currentLockId = currentWriter.get();
////                if (currentLockId != null && !currentLockId.equals(lockId)) { // This should never happen since we don't allow a write lock to be converted back into a read lock
////                    return false;
////                }
//
//                if (writeLockCount.get() > 0) {
//                    System.out.println("Thread = " + Thread.currentThread().getName() + ", acquireReadLock failed, readLockCount = " + readLockCount.get());
//                    return false;
//                }
//
//                if (isProcessing.compareAndSet(false, true)) {
//                    readLockCount.incrementAndGet();
//                    System.out.println("tryAcquireReadLock succeed, lockCount = " + readLockCount.get());
//                    isProcessing.set(false);
//                    return true;
//                }
//                return false;
//            }
//
//            private void releaseReadLock(LockId lockId) {
//                readLockCount.decrementAndGet();
//                System.out.println("releaseReadLock, lockCount = " + readLockCount.get());
//            }
//
//            private boolean tryAcquireWriteLock(LockId lockId) {
////                if (lockId.equals(currentWriter.get())) {
////                    writeLockCount.incrementAndGet();
////                    System.out.println("Thread = " + Thread.currentThread().getName() + ", acquireWriteLock same lockId, write lock count = " + writeLockCount.get());
////                    return true;
////                }
//
//                if ((readLockCount.get() > 0 && lockId.outerLock() == null) || writeLockCount.get() > 0) {
//                    System.out.println("Thread = " + Thread.currentThread().getName() + ", acquireWriteLock failed, readLockCount = " + readLockCount.get());
//                    return false;
//                }
//
//                if (isProcessing.compareAndSet(false, true)) {
//                    writeLockCount.incrementAndGet();
////                    currentWriter.set(lockId);
//                    System.out.println("Thread = " + Thread.currentThread().getName() + ", acquireWriteLock, write lock count = " + writeLockCount.get());
//                    isProcessing.set(false);
//                    return true;
//                }
//                return false;
//            }
//
//            private void releaseWriteLock(LockId lockId) {
//                final var lockCount = writeLockCount.decrementAndGet();
//                System.out.println("Thread = " + Thread.currentThread().getName() + ", releaseWriteLock, lockCount = " + lockCount);
////                if (lockCount == 0) {
////                    currentWriter.set(null);
////                }
//
////                if (lockId.equals(currentWriter.get())) {
////                    if (writeLockCount.decrementAndGet() == 0) {
////                        currentWriter.set(null);
////                    }
////                }
//            }
//
//            private void drainWaiters() {
//                if (WIP.incrementAndGet() == 1) {
//                    LockRequest writeWaiter = writeWaiters.poll();
//                    if (writeWaiter != null) {
//                        writeWaiter.sink().tryEmitEmpty();
//                        return;
//                    }
//
//                    LockRequest readWaiter;
//                    while ((readWaiter = readWaiters.poll()) != null) {
//                        readWaiter.sink().tryEmitEmpty();
//                    }
//                    WIP.decrementAndGet();
//                }
//            }
//        };
//    }
//}
