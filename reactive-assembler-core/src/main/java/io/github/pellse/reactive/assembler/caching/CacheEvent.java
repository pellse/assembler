package io.github.pellse.reactive.assembler.caching;

public sealed interface CacheEvent<R> {
    R value();

    record Updated<R>(R value) implements CacheEvent<R> {
    }

    record Removed<R>(R value) implements CacheEvent<R> {
    }

    static <R> CacheEvent<R> updated(R value) {
        return new Updated<>(value);
    }

    static <R> CacheEvent<R> removed(R value) {
        return new Removed<>(value);
    }
}


