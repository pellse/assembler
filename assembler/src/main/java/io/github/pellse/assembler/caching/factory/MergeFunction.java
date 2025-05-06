package io.github.pellse.assembler.caching.factory;

import io.github.pellse.util.function.Function3;

@FunctionalInterface
public interface MergeFunction<ID, RRC> extends Function3<ID, RRC, RRC, RRC> {
}
