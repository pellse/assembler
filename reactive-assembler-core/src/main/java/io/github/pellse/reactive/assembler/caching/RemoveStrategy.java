package io.github.pellse.reactive.assembler.caching;

import java.util.Map;

@FunctionalInterface
public interface RemoveStrategy<ID, RRC> {

    Map<ID, RRC> remove(Map<ID, RRC> subMapFromCache, Map<ID, RRC> map);
}
