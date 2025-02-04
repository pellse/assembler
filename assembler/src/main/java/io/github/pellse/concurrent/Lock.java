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

import io.github.pellse.util.ObjectUtils;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;

import static io.github.pellse.util.ObjectUtils.doNothing;
import static java.util.Map.entry;
import static reactor.core.publisher.Mono.fromRunnable;

@FunctionalInterface
interface LockFactory<L extends CoreLock<L>> {
    L create(long id, CoreLock<?> outerLock, Consumer<L> lockReleaser);
}

interface Lock<L extends CoreLock<L>> {
    long id();

    CoreLock<?> outerLock();

    Consumer<L> lockReleaser();

    default Mono<?> release() {
        return fromRunnable(() -> lockReleaser().accept(unwrap()));
    }

    @SuppressWarnings("unchecked")
    default L unwrap() {
        return (L) this;
    }

    default String log() {
        return ObjectUtils.toString(this, entry("id", id()), entry("outerLock", outerLock().log()));
    }
}

sealed interface CoreLock<L extends CoreLock<L>> extends Lock<L> {
}

record ReadLock(long id, CoreLock<?> outerLock, Consumer<ReadLock> lockReleaser) implements CoreLock<ReadLock> {
}

record WriteLock(long id, CoreLock<?> outerLock, Consumer<WriteLock> lockReleaser) implements CoreLock<WriteLock> {
}

final class NoopLock implements CoreLock<NoopLock> {

    private static final NoopLock NOOP_LOCK = new NoopLock();

    private NoopLock() {
    }

    static NoopLock noopLock() {
        return NOOP_LOCK;
    }

    @Override
    public long id() {
        return -1;
    }

    @Override
    public CoreLock<?> outerLock() {
        return noopLock();
    }

    @Override
    public Consumer<NoopLock> lockReleaser() {
        return doNothing();
    }

    @Override
    public String log() {
        return "NoopLock";
    }
}