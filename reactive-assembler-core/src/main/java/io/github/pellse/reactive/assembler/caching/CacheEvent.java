package io.github.pellse.reactive.assembler.caching;

public sealed interface CacheEvent<R> {
    R value();

    record AddUpdateEvent<R>(R value) implements CacheEvent<R> {
    }

    record RemoveEvent<R>(R value) implements CacheEvent<R> {
    }

    static <R> CacheEvent<R> add(R value) {
        return new AddUpdateEvent<>(value);
    }

    static <R> CacheEvent<R> remove(R value) {
        return new RemoveEvent<>(value);
    }
}


