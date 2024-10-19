package io.github.pellse.util.concurrent;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Long.MAX_VALUE;
import static java.lang.Thread.onSpinWait;

public interface BoundedQueue<E> {

    E poll();

    void offer(E t);

    long size();

    boolean isEmpty();

    static <E> BoundedQueue<E> createBoundedQueue() {
        return createBoundedQueue(MAX_VALUE);
    }

    static <E> BoundedQueue<E> createBoundedQueue(final long capacity) {

        final var count = new AtomicLong();
        final var queue = new ConcurrentLinkedQueue<E>();

        return new BoundedQueue<>() {

            @Override
            public E poll() {
                E element = queue.poll();
                if (element != null) {
                    count.decrementAndGet();
                }
                return element;
            }

            @Override
            public void offer(E element) {
                boolean done = false;
                while (!done) {
                    long currentSize = count.get();
                    if (currentSize < capacity && count.compareAndSet(currentSize, currentSize + 1)) {
                        done = queue.offer(element);
                    } else {
                        onSpinWait();
                    }
                }
            }

            @Override
            public long size() {
                return count.get();
            }

            @Override
            public boolean isEmpty() {
                return queue.isEmpty();
            }
        };
    }
}
