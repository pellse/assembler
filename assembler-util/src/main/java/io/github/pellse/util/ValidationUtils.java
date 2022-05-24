package io.github.pellse.util;

import java.util.function.IntPredicate;
import java.util.function.Predicate;

public interface ValidationUtils {

    static <T> void validate(int param, IntPredicate predicate, String message) {
        if (!predicate.test(param))
            throw new IllegalArgumentException(message);
    }

    static <T> void validate(T param, Predicate<T> predicate, String message) {
        if (!predicate.test(param))
            throw new IllegalArgumentException(message);
    }

    static IntPredicate isGreaterThan(int value) {
        return v -> v > value;
    }
}
