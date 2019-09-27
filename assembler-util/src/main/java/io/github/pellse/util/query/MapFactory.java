package io.github.pellse.util.query;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public interface MapFactory<ID, R> extends Function<Integer, Map<ID, R>> {

    double MULTIPLIER = 1.34; // To be consistent with the 0.75 default load factor of HashMap

    static <ID, R> MapFactory<ID, R> defaultMapFactory() {
        return size -> new HashMap<>((int)(size * MULTIPLIER));
    }
}
