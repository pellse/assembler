package io.github.pellse.reactive.assembler.caching;

import java.util.Map;
import java.util.function.BiFunction;

@FunctionalInterface
public interface MergeStrategy<ID, RRC> extends BiFunction<Map<ID, RRC>, Map<ID, RRC>, Map<ID, RRC>> {
}
