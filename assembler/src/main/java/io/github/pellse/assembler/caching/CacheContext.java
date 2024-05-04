package io.github.pellse.assembler.caching;

import io.github.pellse.assembler.RuleMapperContext;
import io.github.pellse.assembler.RuleMapperContext.OneToManyContext;
import io.github.pellse.assembler.RuleMapperContext.OneToOneContext;

import java.util.Collection;

import static io.github.pellse.assembler.caching.Cache.*;

public sealed interface CacheContext<ID, R, RRC> {

    boolean isEmptySource();

    RuleMapperContext<?, ?, ID, ?, R, RRC> ctx();

    Cache<ID, RRC> mergeStrategyAwareCache(Cache<ID, RRC> delegateCache);

    record OneToOneCacheContext<ID, R>(
            boolean isEmptySource,
            OneToOneContext<?, ?, ID, R> ctx) implements CacheContext<ID, R, R> {

        @Override
        public Cache<ID, R> mergeStrategyAwareCache(Cache<ID, R> delegateCache) {
            return oneToOneCache(delegateCache);
        }
    }

    record OneToManyCacheContext<ID, R, RC extends Collection<R>>(
            boolean isEmptySource,
            OneToManyContext<?, ?, ID, ?, R, RC> ctx) implements CacheContext<ID, R, RC> {

        @Override
        public Cache<ID, RC> mergeStrategyAwareCache(Cache<ID, RC> delegateCache) {
            return oneToManyCache(ctx(), delegateCache);
        }
    }
}
