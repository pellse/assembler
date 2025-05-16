package io.github.pellse.assembler.caching.merge;

import io.github.pellse.util.function.Function3;

@FunctionalInterface
public interface MergeFunction<ID, RRC> extends Function3<ID, RRC, RRC, RRC> {

    RRC merge(ID id, RRC existingValue, RRC newValue);

    default RRC apply(ID id, RRC existingValue, RRC newValue) {
        return merge(id, existingValue, newValue);
    }
}
