package io.github.pellse.reactive.assembler.caching;

public sealed interface CacheEvent<R> {
    R value();
}

record AddUpdateEvent<R>(R value) implements CacheEvent<R> {
}

record RemoveEvent<R>(R value) implements CacheEvent<R> {
}
