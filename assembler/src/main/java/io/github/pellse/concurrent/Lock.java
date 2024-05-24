package io.github.pellse.concurrent;

import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmitResult;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;

public interface Lock {

    Mono<Token> acquireReadLock();

    Mono<?> releaseReadLock(Token token);

    Mono<Token> acquireWriteLock();

    Mono<?> releaseWriteLock(Token token);

    sealed interface Token {
        long count();
        Token increment();
    }

    record ReadToken(long count) implements Token {
        public ReadToken() {
            this(0);
        }

        public ReadToken increment() {
            return new ReadToken((this.count() + 1));
        }
    }

    record WriteToken(long count) implements Token {
        public WriteToken() {
            this(0);
        }

        public WriteToken increment() {
            return new WriteToken((this.count() + 1));
        }
    }

    //TODO: Store LockRequest(Token, Sinks.Empty), return Mono<Token> instead of Mono<Void>, implement another Lock which capture token,so a reentrant call to writeLock can use Token without having to expose it to the API to the user

    static Lock create() {

        record LockRequest(Token token, Sinks.One<Token> sink) {

            public EmitResult tryEmitValue() {
                return sink().tryEmitValue(token());
            }
        }

        final var readLockCount = new AtomicInteger(0);

        final var readQueue = new ConcurrentLinkedQueue<LockRequest>();
        final var writeQueue = new ConcurrentLinkedQueue<LockRequest>();

        final var writeLockOwner = new AtomicReference<Token>();

        return new Lock() {

            public Mono<Token> acquireReadLock() {
                return acquireReadLock(new ReadToken());
            }

            public Mono<Token> acquireReadLock(Token token) {

                if (token.count() > 0) {
                    readLockCount.incrementAndGet();
                    return Mono.just(token.increment());
                }

                final var sink = Sinks.<Token>one();

                if (canAcquireReadLock()) {
                    readLockCount.incrementAndGet();
                    if (sink.tryEmitValue(token.increment()).isFailure()) {
                        return Mono.error(new IllegalStateException("Failed to emit read lock"));
                    }
                } else {
                    readQueue.offer(new LockRequest(token, sink));
                    processQueues();
                }
                return sink.asMono();
            }

            private boolean canAcquireReadLock() {
                return writeLockOwner.get() == null;
            }

            public Mono<?> releaseReadLock(Token token) {
                return Mono.fromRunnable(() -> {
                    if (readLockCount.decrementAndGet() == 0) {
                        processQueues();
                    }
                });
            }

            public Mono<Token> acquireWriteLock() {
                return acquireWriteLock(new WriteToken());
            }

            public Mono<Token> acquireWriteLock(Token token) {

                if (token.count() > 0) {

//                    Token t = switch(token) {
//                        case ReadToken readToken -> null;
//                        case WriteToken writeToken -> token.increment();
//                    }
//                    return Mono.just(t);
                }

                final var sink = Sinks.<Token>one();

                if (canAcquireWriteLock(token)) {
                    if (sink.tryEmitValue(token).isFailure()) {
                        return Mono.error(new IllegalStateException("Failed to emit write lock"));
                    }
                } else {
                    writeQueue.offer(new LockRequest(token, sink));
                    processQueues();
                }
                return sink.asMono();
            }

            private boolean canAcquireWriteLock(Token token) {
                if ((writeLockOwner.get() == null && readLockCount.get() == 0)) {
                    writeLockOwner.set(token);
                    return true;
                }
                return false;
            }

            public Mono<Token> releaseWriteLock(Token token) {
                return Mono.fromRunnable(() -> {
//                    if (readLockOwners.remove(token) != null) {
//                        processQueues();
//                    }
                });
            }

            private void processQueues() {

                final var nextWriteLock = writeQueue.poll();
                if (nextWriteLock != null) {
                    if (!canAcquireWriteLock(nextWriteLock.token()) || nextWriteLock.tryEmitValue().isFailure()) {
                        writeQueue.offer(nextWriteLock); // Retry if emission fails
                    }
                } else {
                    LockRequest nextReadLock;
                    while ((nextReadLock = readQueue.poll()) != null) {
//                        if (!canAcquireReadLock(nextReadLock.token()) || nextReadLock.tryEmitValue().isFailure()) {
//                            readQueue.offer(nextReadLock);
//                            break;
//                        }
                    }
                }
            }
        };
    }
}
