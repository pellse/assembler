package io.github.pellse.concurrent;

import java.util.function.Consumer;

import static io.github.pellse.concurrent.CoreLock.NoopLock.noopLock;
import static io.github.pellse.util.ObjectUtils.doNothing;
import static java.lang.Thread.currentThread;

public sealed interface CoreLock<L extends CoreLock<L>> extends Lock<L> {

    record ReadLock(long token, CoreLock<?> outerLock, Consumer<ReadLock> lockReleaser, Thread acquiredOnThread) implements CoreLock<ReadLock> {
        ReadLock(long token, Consumer<ReadLock> lockReleaser) {
            this(token, noopLock(), lockReleaser);
        }

        ReadLock(long token, CoreLock<?> outerLock, Consumer<ReadLock> lockReleaser) {
            this(token, outerLock, lockReleaser, currentThread());
        }
    }

    record WriteLock(long token, CoreLock<?> outerLock, Consumer<WriteLock> lockReleaser, Thread acquiredOnThread) implements CoreLock<WriteLock> {
        WriteLock(long token, Consumer<WriteLock> lockReleaser) {
            this(token, noopLock(), lockReleaser);
        }

        WriteLock(long token, CoreLock<?> outerLock, Consumer<WriteLock> lockReleaser) {
            this(token, outerLock, lockReleaser, currentThread());
        }
    }

    final class NoopLock implements CoreLock<NoopLock> {

        private static final NoopLock NOOP_LOCK = new NoopLock();

        private NoopLock() {
        }

        static NoopLock noopLock() {
            return NOOP_LOCK;
        }

        @Override
        public long token() {
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
        public Thread acquiredOnThread() {
            return null;
        }

        @Override
        public String log() {
            return "NoopLock";
        }
    }
}
