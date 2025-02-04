package io.github.pellse.util.function;

@FunctionalInterface
public interface BiLongPredicate<T> {

    boolean test(T t, long value);
}
