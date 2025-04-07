package io.github.pellse.assembler.caching;

import static io.github.pellse.assembler.caching.OptimizedCache.optimizedCache;

public interface OneToOneCache {

    static <ID, R> Cache<ID, R> oneToOneCache(Cache<ID, R> delegateCache) {
        return optimizedCache(delegateCache);
    }
}
