package io.github.pellse.concurrent;

import reactor.core.publisher.Mono;

import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import static io.github.pellse.util.ObjectUtils.doNothing;
import static reactor.core.publisher.Mono.fromRunnable;

sealed interface Lock {
    Lock outerLock();

    Consumer<Lock> releaseLock();

    default Mono<?> release() {
        return fromRunnable(() -> releaseLock().accept(this));
    }

    default Lock unwrap() {
        return this;
    }
}

record ReadLock(Lock outerLock, Consumer<Lock> releaseLock) implements Lock {
}

record WriteLock(Lock outerLock, Consumer<Lock> releaseLock) implements Lock {
}

record NoopLock() implements Lock {

    static NoopLock NOOP_LOCK = new NoopLock();

    @Override
    public Lock outerLock() {
        return NOOP_LOCK;
    }

    @Override
    public Consumer<Lock> releaseLock() {
        return doNothing();
    }
}

record WrapperLock(Lock delegate, UnaryOperator<Consumer<Lock>> releaseLockWrapper) implements Lock {

    @Override
    public Lock outerLock() {
        return delegate.outerLock();
    }

    @Override
    public Consumer<Lock> releaseLock() {
        return releaseLockWrapper.apply(delegate.releaseLock());
    }

    @Override
    public Lock unwrap() {
        return delegate;
    }
}