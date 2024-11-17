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

import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import static io.github.pellse.util.ObjectUtils.doNothing;
import static reactor.core.publisher.Mono.empty;
import static reactor.core.publisher.Mono.fromRunnable;

sealed interface Lock {
    Lock outerLock();

    Consumer<Lock> lockReleaser();

    default Mono<?> release() {
        return fromRunnable(() -> lockReleaser().accept(this));
    }

    default Lock unwrap() {
        return this;
    }
}

record ReadLock(Lock outerLock, Consumer<Lock> lockReleaser) implements Lock {
}

record WriteLock(Lock outerLock, Consumer<Lock> lockReleaser) implements Lock {
}

record NoopLock() implements Lock {

    static NoopLock NOOP_LOCK = new NoopLock();

    @Override
    public Lock outerLock() {
        return NOOP_LOCK;
    }

    @Override
    public Mono<?> release() {
        return empty();
    }

    @Override
    public Consumer<Lock> lockReleaser() {
        return doNothing();
    }
}

record WrapperLock(Lock delegate, UnaryOperator<Consumer<Lock>> lockReleaserWrapper) implements Lock {

    @Override
    public Lock outerLock() {
        return delegate.outerLock();
    }

    @Override
    public Consumer<Lock> lockReleaser() {
        return lockReleaserWrapper.apply(delegate.lockReleaser());
    }

    @Override
    public Lock unwrap() {
        return delegate;
    }
}