package io.github.pellse.reactive.assembler.caching;

import java.util.List;
import java.util.Map;

@FunctionalInterface
public interface MergeStrategy<ID, R> {
    Map<ID, List<R>> merge(Map<ID, List<R>> cache, Map<ID, List<R>> itemsToUpdateMap);
}
