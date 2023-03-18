package io.github.pellse.reactive.assembler.caching;

import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.function.Function.identity;

public sealed interface CacheEvent<R> {
    static <R> Updated<R> updated(R value) {
        return new Updated<>(value);
    }

    static <R> Removed<R> removed(R value) {
        return new Removed<>(value);
    }

    static <T> Function<T, CacheEvent<T>> toCacheEvent(Predicate<T> isUpdated) {
        return toCacheEvent(isUpdated, identity());
    }

    static <T, R> Function<T, CacheEvent<R>> toCacheEvent(Predicate<T> isUpdated, Function<T, R> cacheEventValueExtractor) {
        return source -> toCacheEvent(isUpdated.test(source), cacheEventValueExtractor.apply(source));
    }

    static <R> CacheEvent<R> toCacheEvent(boolean isUpdated, R eventValue) {
        return isUpdated ? updated(eventValue) : removed(eventValue);
    }

    R value();

    record Updated<R>(R value) implements CacheEvent<R> {
    }

    record Removed<R>(R value) implements CacheEvent<R> {
    }
}


