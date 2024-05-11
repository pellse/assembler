package io.github.pellse.assembler.caching;

import io.github.pellse.assembler.RuleMapperContext;
import io.github.pellse.assembler.RuleMapperContext.OneToManyContext;
import io.github.pellse.assembler.RuleMapperContext.OneToOneContext;

import java.util.Collection;

import static io.github.pellse.assembler.caching.Cache.*;
import static io.github.pellse.util.ObjectUtils.also;

public sealed interface CacheContext<ID, EID, R, RRC> {

    boolean isEmptySource();

    RuleMapperContext<?, ?, ID, EID, R, RRC> ctx();

    CacheFactory<ID, EID, R, RRC> mergeStrategyAwareCache(CacheFactory<ID, EID, R, RRC> delegateCacheFactory);

    record OneToOneCacheContext<ID, R>(
            boolean isEmptySource,
            OneToOneContext<?, ?, ID, R> ctx) implements CacheContext<ID, ID, R, R> {

        @Override
        public CacheFactory<ID, ID, R, R> mergeStrategyAwareCache(CacheFactory<ID, ID, R, R> delegateCacheFactory) {
            return ctx -> oneToOneCache(delegateCacheFactory.create(ctx));
        }

        public R convert(R value) {
            return value;
        }
    }

    record OneToManyCacheContext<ID, EID, R, RC extends Collection<R>>(
            boolean isEmptySource,
            Class<RC> collectionType,
            OneToManyContext<?, ?, ID, EID, R, RC> ctx) implements CacheContext<ID, EID, R, RC> {

        @SuppressWarnings("unchecked")
        public OneToManyCacheContext(boolean isEmptySource, OneToManyContext<?, ?, ID, EID, R, RC> ctx) {
            this(isEmptySource, (Class<RC>) ctx.collectionFactory().get().getClass(), ctx);
        }

        @Override
        public CacheFactory<ID, EID, R, RC> mergeStrategyAwareCache(CacheFactory<ID, EID, R, RC> delegateCacheFactory) {
            return cacheContext -> oneToManyCache(ctx(), this::convert, delegateCacheFactory.create(cacheContext));
        }

        @SuppressWarnings("unchecked")
        public RC convert(Collection<R> collection) {
            return collectionType().isInstance(collection) ? (RC) collection : also(ctx().collectionFactory().get(), c -> c.addAll(collection));
        }
    }
}
