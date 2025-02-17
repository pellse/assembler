package io.github.pellse.concurrent;

import java.util.function.Consumer;

import static io.github.pellse.util.ObjectUtils.doNothing;

public sealed interface CoreLock<L extends CoreLock<L>> extends Lock<L> {

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
}
