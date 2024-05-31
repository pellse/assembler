package io.github.pellse.assembler.caching;

import io.github.pellse.assembler.RuleMapperContext;
import io.github.pellse.assembler.RuleMapperContext.OneToManyContext;
import io.github.pellse.assembler.RuleMapperContext.OneToOneContext;

import java.util.Collection;

public sealed interface CacheContext<ID, EID, R, RRC> {

    RuleMapperContext<?, ?, ID, EID, R, RRC> ctx();

    record OneToOneCacheContext<ID, R>(
            OneToOneContext<?, ?, ID, R> ctx) implements CacheContext<ID, ID, R, R> {
    }

    record OneToManyCacheContext<ID, EID, R, RC extends Collection<R>>(
            OneToManyContext<?, ?, ID, EID, R, RC> ctx) implements CacheContext<ID, EID, R, RC> {
    }
}
