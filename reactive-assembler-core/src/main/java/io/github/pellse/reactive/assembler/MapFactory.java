package io.github.pellse.reactive.assembler;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public interface MapFactory<ID, R> extends Function<Integer, Map<ID, R>> {

    static <ID, R> MapFactory<ID, R> defaultMapFactory() {
        return HashMap::new;
    }
}
