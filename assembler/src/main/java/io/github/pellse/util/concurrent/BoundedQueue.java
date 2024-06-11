package io.github.pellse.util.concurrent;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Long.MAX_VALUE;

public interface BoundedQueue<E> {

    E poll();

    boolean offer(E t);

    long size();

    boolean isEmpty();

    boolean isFull();

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
            public boolean offer(E element) {
                while (true) {
                    long currentSize = count.get();
                    if (currentSize >= capacity) {
                        return false;
                    }
                    if (count.compareAndSet(currentSize, currentSize + 1)) {
                        return queue.offer(element);
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

            @Override
            public boolean isFull() {
                return count.get() >= capacity;
            }
        };
    }
}
