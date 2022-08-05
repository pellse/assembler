package io.github.pellse.reactive.assembler.caching;

import java.util.Map;

@FunctionalInterface
public interface MergeStrategy<ID, RRC> {
    Map<ID, RRC> merge(Map<ID, RRC> cache, Map<ID, RRC> itemsToUpdateMap);
}
